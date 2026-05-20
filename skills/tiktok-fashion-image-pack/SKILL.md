---
name: tiktok-fashion-image-pack
description: Generate and QA likeU TikTok Shop fashion product image packs from Feishu bitable records. Use when the user wants OpenClaw to read supplier/product images from Feishu, infer product facts, create high-trust CHUUCHOP-inspired main images for womens tops/jackets with `likeU · product type` microcopy, upload results back to Feishu, or develop/operate this image-generation pipeline.
---

# TikTok Fashion Image Pack

## Overview

Create product-truth-driven TikTok Shop images for likeU. The MVP supports `女装上装/外套`, generates a 1:1 2x2 main image, writes `Product Truth JSON`, prompt, generated attachment, and QA fields back to Feishu.

The system intentionally avoids pure AI fashion posters. It follows this sequence:

1. Read Feishu records with `生成状态=待生成`.
2. Download `原始图片`.
3. Infer `Product Truth JSON`.
4. Build a strict image prompt with `likeU · PRODUCT TYPE` microcopy.
5. Generate one main image through the existing `openai-image` skill.
6. Run visual QA unless skipped.
7. Upload the image and write results back to Feishu.

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

Use `--no-vision` only for debugging; production should use vision-based product truth.

## Feishu Contract

Human-minimal input fields:

- `原始图片`
- `国家`
- `类目`
- `生成图类型`
- `生成状态`
- optional `备注` / `人工覆盖要求`

Set `生成状态` to `待生成` to enqueue a record. Records paused by workflow-only review are also resumable: `生成状态=需人工复核`, `需复核原因=skip-image-generation`, source images present, and no `首图结果`.

System fields include `Product Truth JSON`, `生成Prompt`, `首图结果`, `质检结果`, `质检问题`, and AI-expanded product facts. See `references/field_schema.md` when editing the schema.

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
