# Category Isolation Migration

## Scope

This migration moves market-insight and product-selection outputs from implicit
country/category fields to explicit `market_id` + `category_id` isolation.

## Required Steps

1. Prefix existing hair-accessory direction IDs with `VN__hair_accessory__`
   where the historical source is the Vietnam hair-accessory run.
2. Backfill `market_id` and `category_id` in Feishu direction-card tables,
   SQLite snapshots, JSONL artifacts, and sample-pool rows.
3. Preserve a mapping table from old direction names to canonical IDs.
4. Add `market_id/category_id` to market report history rows.
5. Add `market_id/category_id` to direction sample pool rows.
6. Add `market_id/category_id` to V2 shadow scoring rows.
7. Run `scripts/migrate_category_isolation.py --dry-run` before applying.
8. Keep rollback script available for artifact-level restore.

## Initial Mapping

```yaml
direction_id_migration:
  "少女礼物感型": "VN__hair_accessory__sweet_gift"
  "盘发效率型": "VN__hair_accessory__hair_up_efficiency"
  "头盔友好整理型": "VN__hair_accessory__helmet_friendly"
  "甜感装饰型": "VN__hair_accessory__sweet_decorative"
  "大体量气质型": "VN__hair_accessory__volume_elegance"
  "韩系轻通勤型": "VN__hair_accessory__korean_light_commute"
  "基础通勤型": "VN__hair_accessory__basic_commute"
  "发箍修饰型": "VN__hair_accessory__headband_shape"
  "发圈套组型": "VN__hair_accessory__hair_tie_set"
```

## Rollback

Rollback must restore the artifact/database backup created before migration and
remove newly generated `market_id/category_id` columns only from the migrated
copy, never from manually edited Feishu fields.
