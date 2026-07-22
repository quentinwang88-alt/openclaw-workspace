# Wig and Hairpiece Category — Mexico

## Scope

Use `假发` as the umbrella category for `国家=MX`, then route by `product_form`:

- lace-front wigs
- full-cap wigs
- U-part wigs
- headband wigs
- ponytail pieces
- clip-in extensions
- hair toppers
- human hair, synthetic fiber, heat-resistant synthetic, blend, or unknown fiber
- straight, wavy, and curly textures
- single-color and observed ombre/highlight variants

Never route ponytail pieces, clip-in extensions, or toppers through the full-wig front/back or cap-construction template. Do not make medical claims.

## Listing vs Marketing Outputs

The production image pack is a TikTok Shop Mexico listing pack:

- square images
- full-wig hero uses a front-worn Mexican/Latina model plus a complete back-view proof
- ponytail hero uses worn effect plus the complete detached ponytail and sourced attachment proof
- clip-in hero uses worn effect plus the actual detached wefts/pieces
- topper hero uses top/front coverage plus the complete detached topper/base
- all hero images use white/soft-cream backgrounds
- no text, logo, border, watermark, price, badges, icons, or promotional graphics in any W slot
- brand-poster references may guide off-platform marketing assets only; they are never product-truth references

## Product Truth

Wig truth must preserve:

- fiber type and evidence level
- construction type and lace area
- hairline and parting
- length and density only when confirmed
- texture, curl pattern, layers, bangs, and ends
- main/root color, ombre boundary, and highlight placement
- cap size/features, combs, clips, straps, and adjustment hooks only when sourced
- heat resistance and temperature only when sourced
- observed pack contents only
- product form, piece count, attachment/base shape, and fixing parts only when sourced

`cabello humano`, `resistente al calor`, density, lace dimensions, cap size, and numeric length are evidence-gated claims.

## W Slots

- `W1`: product-form-specific worn effect
- `W2`: full-wig back proof or complete detached hairpiece proof
- `W3`: hairline/lace/parting or sourced base/weft/fiber detail
- `W4`: cap/comb/drawstring/clip/base proof; fallback to clean side/profile product view
- `W5`: fiber, color-gradient, curl, layers, and ends detail
- `W6`: everyday Mexico lifestyle; observed color-options proof for multicolor products

Blank/default legacy S1-S4 selections map to W1-W6 for wigs. Explicit partial selections map S1-S6 to the matching W slots.

## Source Sufficiency Gate

- No clear hairline reference: `has_hairline_reference=false`; W3 must not invent lace, knots, scalp, or baby hair.
- No clear cap reference: `has_cap_construction_reference=false`; W4 must use the side/profile fallback.
- No clear attachment/base reference: `has_attachment_reference=false`; ponytail/clip-in/topper W4 must use a safe product-view fallback.
- No real full-wig back reference: `has_back_reference=false`; label generated back views as visual inference in review reasons.
- Missing high-risk evidence is written to `review_reasons` and `假发结构参考充分性`.

## Title

Mexico title prompt: `prompts/假发_墨西哥.md`.

Preferred structure:

`Peluca + construction/audience + length/texture + proven fiber + color/gradient + proven usage/structure`

Use `fibra sintética` for synthetic fiber, `cabello humano` only when confirmed, and `aspecto natural` for natural-looking appearance.

## Pilot

Before production rollout, dry-run and visually inspect 8-12 records covering:

- lace front vs full cap
- synthetic vs human hair
- straight vs wavy/curly
- solid color vs ombre/highlights
- with and without hairline/cap source images

Tune prompts based on root-color drift, moved highlights, curl-size drift, fake scalp/lace, invented cap parts, and model realism.
