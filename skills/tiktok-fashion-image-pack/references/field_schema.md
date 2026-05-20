# Feishu Field Schema

Human input fields:

- `原始图片`: attachment, one or more supplier/product images.
- `国家`: target market, default `TH`.
- `类目`: first-phase value should be `女装上装/外套`.
- `生成图类型`: `只首图` or `首图+详情图`; MVP processes only main image.
- `生成状态`: set to `待生成` to enqueue a record.
- `备注` / `人工覆盖要求`: optional override notes.

System output fields:

- `Product Truth JSON`: source-of-truth JSON for image generation.
- `生成Prompt`: exact prompt sent to image generation.
- `首图结果`: generated main image attachment.
- `质检结果`, `质检问题`, `需复核原因`: QA writeback.

Brand fields:

- `品牌名`: defaults to `likeU`.
- `主图小字策略`: defaults to `likeU + 产品类型`.
- `主图小字名称`: optional product type override. If empty, AI infers a type such as `SUEDE JACKET`.
