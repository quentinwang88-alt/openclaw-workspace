# AutoMixcut V1.0 Prompts

## Product Anchor Prompt

Generate a JSON product anchor card from product title, category, market, main image, and selling points. Return `category`, `product_subtype`, `core_visual_points`, `must_not_change_points`, `forbidden_mismatch`, `strict_roles`, and `allowed_scene_usage`.

## Watermark Detection Prompt

Inspect sampled frames and decide whether TikTok/Douyin logo, platform UI, user ID, account name, repost mark, or obvious watermark is visible. Return `has_watermark`, `watermark_type`, `watermark_position`, `watermark_confidence`, and `watermark_reason`.

## Segment Tagging Prompt

Tag one segment from sampled frames. Return `primary_shot_role`, `secondary_roles`, `product_visibility`, `hook_strength`, `mixcut_usability`, `risk_level`, `confidence`, `needs_human_review`, and `reason`.

## AI Generated Consistency Prompt

Compare 8-10 sampled frames from an AI generated segment. Score whether product shape, structure, and key visual points drift across frames. Return `frame_consistency_score`, `frame_consistency_status`, and `frame_consistency_reason`.
