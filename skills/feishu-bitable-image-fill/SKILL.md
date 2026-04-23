---
name: feishu-bitable-image-fill
description: Fill a Feishu bitable attachment image field from existing image URLs or product/detail page links. Use when the user provides a Feishu bitable/wiki link and asks to 补图片, 回填图片, 补充图片, 把商品图片放进附件列, or fill an attachment column such as `图片` from fields like `商品图片`, `TikTok商品落地页地址`, or `FastMoss商品详情页地址`.
---

# Feishu Bitable Image Fill

Use this skill to backfill a Feishu bitable attachment field with image files fetched from URL fields.

## Workflow

1. Inspect the table fields first when the source or target column is not obvious.
2. Prefer the bundled script:
   `scripts/fill_bitable_image_attachments.py`
3. Default source-field priority is:
   - `商品图片`
   - `TikTok商品落地页地址`
   - `FastMoss商品详情页地址`
4. Default target field is:
   - `图片`
5. Do not use `--overwrite` unless the user explicitly asks to replace existing attachments.
6. When the table is unfamiliar, large, or risky, run a dry-run first.
7. When the user provides a Feishu link with `view=...`, let the script use that view filter by default.

## Recommended commands

### Standard run

```bash
python3 skills/feishu-bitable-image-fill/scripts/fill_bitable_image_attachments.py \
  --url '<FEISHU_BITABLE_URL>'
```

### Dry-run first

```bash
python3 skills/feishu-bitable-image-fill/scripts/fill_bitable_image_attachments.py \
  --url '<FEISHU_BITABLE_URL>' \
  --dry-run
```

### Custom source / target fields

```bash
python3 skills/feishu-bitable-image-fill/scripts/fill_bitable_image_attachments.py \
  --url '<FEISHU_BITABLE_URL>' \
  --source-field '商品主图' \
  --source-field '详情页链接' \
  --target-field '图片'
```

### Force a specific view

```bash
python3 skills/feishu-bitable-image-fill/scripts/fill_bitable_image_attachments.py \
  --url '<FEISHU_BITABLE_URL>' \
  --view-id 'vewxxxx'
```

## Notes

- The script uploads actual files into the Feishu attachment field, not just URL text.
- It can fetch direct image URLs and can also extract `og:image` from page links.
- It writes in batches and sleeps briefly between records to stay gentler on external APIs.
- If one source field fails for a record, the script automatically tries the next source field.
- The script exits non-zero when any record ultimately fails, so read the summary before reporting results.
