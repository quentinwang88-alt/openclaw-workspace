# Original Script Generator Agent Guide

## Scope
- This repo evolves the current pipeline; do not redesign the system from scratch.
- Keep the production pipeline backward compatible.
- The reality-reference stage-0 path is an isolated experiment: no Feishu writes,
  no video generation, no variants, and a separate voiceover SQLite database.

## Core Principle
- Prioritize `原生感成立 + 避免明显广告化`.
- Do not optimize for “强压广告感” as the main goal.
- The content should feel like real sharing, not a product manual.

## Direction Rule
- `S1 / S2 / S3 / S4` are compatibility output slots, not fixed creative
  directions or fixed visual styles.
- A direction is defined by its structure contract plus its selected observed
  execution reference.  Return fewer directions when compatible observed
  references are insufficient; never invent evidence to fill four slots.
- Direction diversity must be visible in executable actions, carrier, camera
  grammar and continuity, not in abstract personality labels.

## Strategy Card Requirements
- The legacy production path may still consume the full P2 schema.
- The reality-reference path uses a compact content-bundle brief: one coherent
  mainline with two to three non-redundant, verified claim atoms and one observed
  proof relationship.  P2-Lite remains only as a compatibility projection. Do
  not add persona, styling, scene or emotion fields back into this compact layer.

## Script Requirements
- In the reality-reference path, visuals are generated before voiceover.
- Every claim atom in the content bundle must be assigned to at least one
  objectively supporting shot before the central voiceover engine is called.
- Every shot must map to the exact structure plan and cite one or more
  `reference_spine_orders`; observed order may be repeated but not inverted.
- Unknown source properties remain unknown in `execution_reference`.  A later
  complete-script blueprint may add location, lighting, creator identity and
  styling only as explicitly labelled `CREATIVE_DESIGN`; those choices must
  never be written back as source observations.
- Product-specific anchors may replace the source product, but the action must
  remain physically compatible with the target product.
- Do not fall back to the legacy mirror / step-back / look-down / turn chain.
- Do not require an abstract emotion arc.  A complete-script blueprint may
  define concrete creator motivation, visible behaviour and speaking identity
  when each field has an explicit downstream consumer.

## Creative Diversity Rule
- Allocate `persona_role × scene_motif × opening_action` in code before asking
  a model to write the complete blueprint.
- Compare against recent historical usage, not just the current batch.
- A failed combination may be quarantined; its individual axes are not
  permanent bans and may return through a different, genuinely distinct
  combination.
- Stage-0 text that passes rules remains `MACHINE_SCREENED`; it is not approved
  for release until independent, native-language and content-human review pass.

## QC Requirements
- Reality-reference QC must catch missing execution lineage, inverted observed
  order, abstract AI/meta instructions, legacy action chains, empty/generic
  hooks and product claims that have no support anywhere in the whole video.
- Voiceover is planned in semantic segments, not sentence-to-shot annotations.
  Local timing/order mismatches are warnings; only an explicit MUST_SILENT
  interval blocks speech. A natural audience/need callout may lead the visuals.
- The opening must have an audible attention move. It does not have to name the
  first-shot observation word for word, but empty taxonomy copy is not a pass.

## Video Prompt Requirements
- Final video prompt must stay compact.
- Preserve the observed execution relationship and product anchor constraints.
- Do not re-expand into strategy explanation.

## Engineering Rule
- When updating prompts, also update:
  - `core/json_parser.py`
  - `core/script_renderer.py`
  - any related validation or recovery logic
- Keep backward-compatible fallbacks where practical, but bias new generations toward the upgraded schema.
