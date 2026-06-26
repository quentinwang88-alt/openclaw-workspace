from __future__ import annotations


class SourceType:
    AI_GENERATED = "ai_generated"
    SELF_SHOT = "self_shot"
    AUTHORIZED_CREATOR = "authorized_creator"
    CREATOR_AUTHORIZED = "creator_authorized"
    ORIGINAL_SCRIPT = "original_script"
    CREATOR_ORIGINAL = "creator_original"
    DOUYIN_REPOST = "douyin_repost"
    COMPETITOR = "competitor"
    OTHER = "other"


TRUSTED_REAL_SOURCE_TYPES = {
    SourceType.AUTHORIZED_CREATOR,
    SourceType.SELF_SHOT,
    SourceType.ORIGINAL_SCRIPT,
    SourceType.CREATOR_ORIGINAL,
}

LOW_TRUST_REFERENCE_SOURCE_TYPES = {
    SourceType.DOUYIN_REPOST,
    SourceType.COMPETITOR,
}
