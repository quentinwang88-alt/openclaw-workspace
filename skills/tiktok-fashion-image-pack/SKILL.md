---
name: tiktok-fashion-image-pack
description: Generate and QA likeU TikTok Shop fashion product image packs from Feishu bitable records. Use when the user wants OpenClaw to read supplier/product images from Feishu, infer product facts, create high-trust CHUUCHOP-inspired main images for womens tops/jackets with `likeU · product type` microcopy, upload results back to Feishu, or develop/operate this image-generation pipeline.
---

# TikTok Fashion Image Pack

## Overview

Create product-truth-driven TikTok Shop images for likeU. The MVP supports `女装上装/外套`, generates 1:1 main images and optional S1-S4 lifestyle scene images, writes `Product Truth JSON`, prompts, generated attachments, and QA fields back to Feishu.

The system intentionally avoids pure AI fashion posters. It follows this sequence:

1. Read Feishu records with `生成状态=待生成`.
2. Download `原始图片`.
3. Infer `Product Truth JSON`.
4. Route by `生成图类型`.
5. Build a strict main-image prompt with `likeU · PRODUCT TYPE` microcopy when needed.
6. Build S1-S4 scene prompts when needed.
7. Generate images through the existing `openai-image` skill.
8. Run visual QA unless skipped.
9. Upload images and write results back to Feishu.

## Quick Start

Run one pending record:

```bash
cd /Users/likeu3/.openclaw/workspace/skills/tiktok-fashion-image-pack
python3 run_pipeline.py \
  --feishu-url 'https://gcngopvfvo0q.feishu.cn/wiki/DYHpwJx39iRSnTk5ZkpcPau8nsb?table=tbli89dtn8tOgYdI&view=vewSovaI4K' \
  --limit 1
```

Safe prompt-only test:

```bash
python3 run_pipeline.py --feishu-url '<url>' --limit 1 --dry-run
```

Write product truth and prompt but skip image generation:

```bash
python3 run_pipeline.py --feishu-url '<url>' --limit 1 --skip-image-generation
```

Run title-only (skip image generation):

```bash
python3 run_pipeline.py --feishu-url '<url>' --title-only
```

If `标题状态=待生成` is set on a record, title generation runs automatically after image pipeline. Use `--skip-title` to disable this behavior. Use `--overwrite-title` to force regenerate an already-completed title.

```bash
# Skip title generation even when title_status=待生成
python3 run_pipeline.py --feishu-url '<url>' --limit 1 --skip-title

# Force regenerate title
python3 run_pipeline.py --feishu-url '<url>' --limit 1 --overwrite-title
```

Use `--no-vision` only for debugging; production should use vision-based product truth.

## Feishu Contract

Human-minimal input fields:

- `原始图片`
- optional `原始场景参考图`
- `国家`
- `类目`
- `生成图类型`
- `生成状态`
- optional `备注` / `人工覆盖要求`

Set `生成状态` to `待生成` to enqueue a record. Records paused by workflow-only review are also resumable: `生成状态=需人工复核`, `需复核原因=skip-image-generation`, source images present, and no `首图结果`.

Supported `生成图类型` values:

- `只首图`: generate only the main product image.
- `只场景图`: generate only S1-S4 scene images.
- `首图+场景图`: generate main image, then S1-S4 scene images.
- `首图+详情图`: legacy value; currently behaves as main image only.
- `全套图包`: currently behaves as main image plus S1-S4 scene images.

System fields include `Product Truth JSON`, `生成Prompt`, `场景图Prompt`, `首图结果`, `场景图结果`, QA fields, and AI-expanded product facts. See `references/field_schema.md` when editing the schema.

`原始场景参考图` is for background, lighting, pose, and composition references only. Product truth must always come from `原始图片`.

## Womens Tops MVP

Use `references/womens_tops_rules.md` for category behavior. The first phase supports leather, suede, utility, puffer, faux fur, knit top, and cardigan subtypes.

Main image layout is selected automatically:

- `womens_tops_multicolor_triptych`: multi-color products. IMAGE 1 is the promoted hero color; other colors appear only as compact product-only options.
- `womens_tops_product_only_to_tryon_truth_split`: references without real on-body model, such as hanger, flat-lay, white-background, or pure product images. Uses faceless cropped try-on plus product-only proof and details.
- `womens_tops_single_hero_detail_sidebar`: default single-color outerwear. Large on-body hero plus styling/detail sidebar.
- `womens_tops_structure_feature_split`: single-color products with visible structure selling points such as pockets, buttons/snaps, zipper, belt, drawstring, hardware, bomber/baseball details.
- `womens_tops_material_mood_split`: single-color texture-led products such as faux fur, suede, knit, PU leather, plush, or fleece when structure is not the main selling point.

## QA

QA compares source image and generated image against `Product Truth JSON`. It rejects material changes, invented structural details, misleading accessories, and obvious AI distortions. See `references/qa_rules.md`.

If QA fails, the runner retries once by default with the issue list appended to the prompt. After repeated failure, it writes `需人工复核`.

## Implementation Notes

- Reuse `/Users/likeu3/.openclaw/workspace/skills/openai-image` for `gpt-image-2`.
- Keep `Product Truth JSON` as the generation contract; do not bypass it.
- Default brand microcopy is `likeU · PRODUCT TYPE` in English. Do not translate product type microcopy into Thai.
- Do not add non-sold accessories in product-only panels.
- Scene images are not generated from the already-generated main image. Use supplier references plus `Product Truth JSON`; the main image can be treated only as style context in future extensions.
- Default scene slots are `S1,S2,S3,S4`: hero lifestyle try-on, daily use atmosphere, fit/color proof, and material/construction detail.
- Multi-color products auto-expand from default S1-S4 to S1-S6. S5 and S6 are alternate observed-color try-on images, so multi-color SKUs provide 2-3 worn color references.
- Scene preferences are optional. Blank or `自动匹配` uses automatic scene choice.

## Feedback Fix

Human-driven image correction pipeline. When generated images have issues, write natural-language feedback in the Feishu record and the system regenerates targeted fixes.

### How to use

1. In the Feishu record that already has generated images, fill in:
   - `反馈目标图`: which image(s) to fix (`首图`, `S1`-`S6`)
   - `图片反馈问题`: write what's wrong and how to fix it in natural language
   - `反馈处理方式`: `局部修正` (default, only fix pointed issues) or `整图重生`
   - `反馈状态`: set to `待修正`

2. Run the feedback pipeline:

```bash
python3 run_pipeline.py --feedback-only --feishu-url '<url>' --limit 1
```

```bash
# Dry-run to preview the fix prompt
python3 run_pipeline.py --feedback-only --feishu-url '<url>' --limit 1 --dry-run
```

### Feedback writing guide

Write in the format: `问题位置 + 问题描述 + 修正要求`

Examples:
```
S1 模特脸太假，改成轻微侧脸或低头，不要正脸精修感；衣服版型、口袋和扣子参考原图。
```
```
首图 产品颜色偏浅，改回原图黑色；小字缩小一点，不要压到衣服主体。
```
```
S3 背景太像棚拍，改成真实商场/咖啡店门口自然光；不要新增包包和帽子。
```
```
S2 扣子错了，原图是黑色暗扣，不是金色扣；请改回黑色暗扣，其它部分尽量不变。
```

### Priority rules

```
人工反馈 > 原始商品图 > 商品识别信息 > 上一版生成图
```

- Issues explicitly pointed out in feedback **must** be fixed.
- Original product images are the highest authority for product facts (color, material, silhouette, buttons, pockets, collar, etc).
- Previous generated image is only used for composition, pose, lighting, and atmosphere reference.
- Unmentioned elements should remain unchanged.

### Fix methods

- **局部修正**: Only fix the issues pointed out. Composition, lighting, and unmentioned details stay as-is.
- **整图重生**: May adjust composition/lighting/atmosphere as needed, but product truth still comes from original images.

### Output

- `反馈修正结果`: corrected main image (separate from `首图结果` for comparison)
- `反馈修正结果_场景图`: corrected scene images (separate from `场景图结果` for comparison)
- `反馈修正Prompt`: the exact prompt used for the fix
- `反馈状态`: updated to `已修正` or `需人工复核`
