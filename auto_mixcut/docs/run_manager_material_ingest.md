# Run Manager Material Ingest

The shared Feishu run-manager table is a transient generation queue. Generated
videos are archived to OSS and registered in `auto_mixcut`; mixcut outputs are
explicitly excluded from this material intake.

## Safety defaults

- All commands are dry-run unless `--apply` is supplied.
- Feishu attachments are never cleared unless both
  `MATERIAL_FEISHU_ATTACHMENT_CLEANUP_ENABLED=1` and `--clear-attachments` are
  supplied.
- Cross-market selection is disabled until
  `MATERIAL_CANONICAL_PRODUCT_SELECTION_ENABLED=1` is set.
- Publisher and light-video OSS fallback are independently gated by
  `PUBLISH_OSS_SOURCE_ENABLED=1` and `LIGHT_VIDEO_OSS_SOURCE_ENABLED=1`.

## Schema and Feishu fields

```bash
python3 scripts/ensure_run_manager_material_fields.py --source run-manager --dry-run
python3 scripts/ensure_run_manager_material_fields.py --source run-manager
python3 scripts/ensure_run_manager_material_fields.py --source light-review
```

The database schema is applied through the normal `RDSRepositorySkill.init_db`
path. Migration `005_run_manager_material_registry.sql` adds the source registry,
product aliases, and canonical product fields.

## Shadow scan and import

```bash
python3 scripts/sync_run_manager_materials.py scan \
  --cutover-at '2026-07-25T00:00:00' \
  --limit 20

python3 scripts/sync_run_manager_materials.py scan \
  --cutover-at '2026-07-25T00:00:00' \
  --limit 20 \
  --apply
```

Index historical rows without moving media:

```bash
python3 scripts/sync_run_manager_materials.py index --limit 100 --apply
```

Activate selected historical material:

```bash
python3 scripts/sync_run_manager_materials.py activate \
  --canonical-product-id SYC001 \
  --source-flow video_remake \
  --date-from 2026-06-01 \
  --limit 20
```

Repeat with `--apply` only after inspecting the preview.

Import clean source videos from the light-video review table. When both
`初始成片` and `最终视频` exist, only `初始成片` is selected. A historical row
with only `最终视频` is indexed as legacy material and does not enter the active
mixcut pool unless an operator explicitly activates it:

```bash
python3 scripts/sync_light_review_materials.py scan \
  --canonical-product-id 1736444730937804794 \
  --limit 20

python3 scripts/sync_light_review_materials.py scan \
  --canonical-product-id 1736444730937804794 \
  --limit 20 \
  --apply
```

## Product identity

Resolve an execution product:

```bash
python3 scripts/manage_product_identity.py resolve --product-id TH_LOCAL_1
```

Preview and bind a cross-market alias:

```bash
python3 scripts/manage_product_identity.py bind \
  --canonical-product-id SYC001 \
  --product-id US_LOCAL_2 \
  --local-product-id US-LISTING-2 \
  --store-id US01 \
  --market US

python3 scripts/manage_product_identity.py bind \
  --canonical-product-id SYC001 \
  --product-id US_LOCAL_2 \
  --local-product-id US-LISTING-2 \
  --store-id US01 \
  --market US \
  --apply
```

## Rollout order

1. Apply database and Feishu fields.
2. Run shadow scans.
3. Import a small batch with attachment cleanup disabled.
4. Enable publisher and light-video OSS fallback and verify retries.
5. Enable canonical material selection for one product.
6. Only then enable attachment cleanup.
