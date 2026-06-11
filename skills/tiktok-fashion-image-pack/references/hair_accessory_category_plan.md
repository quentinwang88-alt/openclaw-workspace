# Hair Accessory Category Extension Plan

## Scope

Add `发饰` as the next supported category after `女装上装/外套`.

The first production target is TikTok Shop Thailand and Vietnam. Keep the system lightweight: use stable marketplace search terms, product-truth extraction, category-specific title prompts, and category-specific image layouts. Do not build a heavy daily trend pipeline until the category has stable order volume.

## Category Router

Add a category routing layer before product truth, prompt building, scene prompt building, title generation, and QA.

Recommended behavior:

- `女装上装/外套`: use existing womens tops flow.
- `发饰`: use hair accessory flow.
- Other categories: write `需人工复核` or `暂未支持类目` until implemented.

Shared infrastructure can be reused:

- Feishu table IO.
- attachment download/upload.
- model circuit breaker.
- feedback fix flow.
- generated image QA framework.
- title output fields.

## Product Truth

Hair accessory truth fields should focus on accessory facts rather than clothing structure.

Required fields:

- `category`: `hair_accessory`.
- `subtype`: `hair_clip | claw_clip | hair_bow | headband | scrunchie | hair_tie | hair_pin | unknown_hair_accessory`.
- `product_type_name_en`: short English label for image microcopy.
- `product_type_name_zh`: Chinese subtype name for operator review.
- `source_image_type`: `on_body_model | product_only | flat_lay | white_bg | mixed | unknown`.
- `main_color`.
- `is_probably_multicolor`.
- `sellable_colors_observed`.
- `material`: satin, velvet, acrylic, plastic, metal, pearl-like, rhinestone-like, lace, fabric, etc.
- `size_scale`: small, medium, large, oversized, mini.
- `wearing_position`: side hair, back bun, ponytail, hairline, bangs, full headband.
- `grip_structure`: clip, claw teeth, snap clip, elastic, headband frame, bow clip.
- `decorative_elements`: bow, pearl, rhinestone, flower, solid color, pattern.
- `pack_count`: only when visibly or explicitly confirmed.
- `core_selling_points`.
- `must_preserve`.
- `must_not_add`.
- `confidence`.
- `review_reasons`.

Important rules:

- Do not infer set count from display styling unless clear.
- Do not claim real silk, real pearl, real crystal, or branded IP unless confirmed.
- Preserve scale; hair accessories become fake-looking when size is exaggerated.

## Main Image Layouts

Use category-specific templates:

- `hair_accessory_worn_closeup_split`: hero worn close-up plus product-only detail panel.
- `hair_accessory_product_detail_split`: product-only hero plus hand/scale/detail panels for references without wearable context.
- `hair_accessory_multicolor_options`: promoted color in the hero area, other colors as compact product-only options.
- `hair_accessory_set_flatlay`: for confirmed multi-piece sets; show pack contents clearly.

Default principle:

- If a real wearing reference exists, use a cropped hair/side/back-head close-up rather than full-face beauty portrait.
- If only product-only reference exists, create a weak-human-context image such as hand holding, hair-back close-up, or product on vanity surface; avoid perfect AI faces.
- Non-sold accessories should not compete with the hair accessory.

## Scene Slots

Use H slots for hair accessories instead of womenswear S slots:

- `H1`: worn close-up hero, side/back/partial-face crop.
- `H2`: hairstyle matching scene, daily/cafe/street/mirror context.
- `H3`: product detail and material close-up.
- `H4`: scale/usage proof or product-only clean layout.
- `H5`: alternate color worn effect for multi-color products.
- `H6`: second alternate color or set contents proof.

For multi-color products, generate 6 scene images and ensure at least 2-3 worn effects cover different colors.

## Title Keywords

Keyword files:

- Thailand and Vietnam hair accessory terms are stored in `references/title_keywords_hair_accessories.json`.
- Existing womens outerwear terms remain in `references/title_keywords_th_womens_outerwear.json`.

Keyword groups:

- `core_terms`: stable local search terms and category terms.
- `material_terms`: material or appearance.
- `fit_terms`: size, comfort, scale.
- `structure_terms`: grip, hold, usage.
- `style_terms`: lightweight style modifiers.

Title prompt templates:

- Thailand: `prompts/发饰_泰国.md`.
- Vietnam: `prompts/发饰_越南.md`.

Title structure:

`[core product term] + [material/appearance] + [size/comfort] + [real usage point] + [one style/context term]`

Title constraints:

- No shop name, including likeU.
- No promotion words.
- No series code unless explicitly requested.
- No fake material claims.
- No pack count unless confirmed.
- Style words should not overwhelm the core searchable term.

## Lightweight Keyword Refresh

Recommended cadence:

- Stable keyword library: update monthly or when launching a new subtype.
- No daily keyword scraping in the first stage.
- Collect terms from TikTok Shop, Shopee, Lazada, and real competitor titles.
- Add only recurring terms seen across multiple listings or platforms.

Useful stable Thai terms:

- `กิ๊บติดผม`, `กิ๊บหนีบผม`, `ที่คาดผม`, `ยางมัดผม`, `ยางรัดผม`, `โบว์ติดผม`, `เครื่องประดับผม`.

Useful stable Vietnamese terms:

- `kẹp tóc`, `kẹp tóc càng cua`, `kẹp mái`, `băng đô`, `cài tóc`, `scrunchies`, `dây buộc tóc`, `thun cột tóc`, `nơ tóc`, `phụ kiện tóc`.

## Current Implementation Status

Done:

- Added `references/title_keywords_hair_accessories.json`.
- Added Thailand and Vietnam hair accessory title prompts.
- Updated title keyword loader to support `category + country + subtype`.
- Updated title optimizer to choose category/country title templates.
- Updated title QA to avoid rejecting Vietnamese titles for missing Thai characters.
- Updated pipeline title generation to pass `类目` and `国家`.
- Added product truth extractor for `发饰`.
- Added category-aware main-image prompts and layouts for hair accessories.
- Added H-slot scene prompt builder for hair accessories.
- Added hair accessory visual QA prompts.
- Added Feishu scene slot options for H1-H6.
- Added compatibility mapping from existing S1-S6 slot selections to H1-H6 when `类目=发饰`.
- Added feedback correction compatibility for H1-H6 target labels.
- Updated feedback correction prompts and QA categories for hair accessory details.

Still required before image generation for hair accessories:

- Run an actual Feishu/OpenClaw image-generation pilot with 2-3 hair accessory records.
- Inspect generated images for product scale, fake-face risk, accessory visibility, and unobserved set quantity.
- After the pilot, tune the H-slot prompt wording based on actual visual failure patterns.
