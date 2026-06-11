# Feishu Field Schema

Human input fields:

- `原始图片`: attachment, one or more supplier/product images.
- `原始场景参考图`: optional attachment, scene/style references only. These do not participate in product truth recognition.
- `国家`: target market, default `TH`.
- `类目`: supported values include `女装上装/外套` and lightweight `发饰`.
- `生成图类型`: `只首图`, `只场景图`, `首图+场景图`, `首图+详情图`, or `全套图包`.
- `生成状态`: set to `待生成` to enqueue a record.
- `备注` / `人工覆盖要求`: optional override notes.

System output fields:

- `Product Truth JSON`: source-of-truth JSON for image generation.
- `生成Prompt`: exact prompt sent to image generation.
- `场景图Prompt`: exact category scene prompt package sent to image generation.
- `首图结果`: generated main image attachment.
- `场景图结果`: generated scene image attachments.
- `质检结果`, `质检问题`, `需复核原因`: QA writeback.
- `场景图质检结果`, `场景图质检问题`, `场景图生成明细`: scene image QA and per-slot run details.
- `场景偏好`: optional single select. Blank or `自动匹配` lets AI choose from product/category/country.
- `场景图槽位`: optional multi select. Blank means S1-S4 for womenswear and H1-H4 for hair accessories. Selected values can include `S1 主点击试穿`, `S2 日常氛围`, `S3 版型/颜色证明`, `S4 材质结构细节`, `H1 发饰佩戴近景`, `H2 发型搭配场景`, `H3 发饰细节比例`, `H4 发饰颜色/商品证明`.

Brand fields:

- `品牌名`: defaults to `likeU`.
- `主图小字策略`: defaults to `likeU + 产品类型`.
- `主图小字名称`: optional product type override. If empty, AI infers a type such as `SUEDE JACKET` or `CLAW CLIP`.

Title fields:

- `原标题/供应商标题`: optional original supplier title for title generation.
- `标题生成状态`: set to `待生成` to auto-generate title after image pipeline.
- `TK标题`: generated TikTok Shop title in target language.
- `标题中文摘要`: Chinese summary of the generated title for human review.
- `标题关键词`: keywords used in the generated title.
- `标题系列`, `标题系列编码`: series name and code (e.g. 皮衣系列/E).
- `标题质检结果`, `标题质检问题`: title QA writeback.
- `标题生成Prompt`: exact prompt sent for title generation.
- `标题生成时间`: timestamp of last title generation.
- `标题人工要求`: optional human override notes for title generation.

Feedback fix fields:

- `反馈目标图`: multi-select, which image(s) to fix (`首图`, `S1`-`S6`, or `H1`-`H6` when supported by the correction flow).
- `图片反馈问题`: long text, human-written issue description in natural language.
- `反馈状态`: single select, drives the fix pipeline. Set to `待修正` to enqueue. Values: `待修正`, `修正中`, `已修正`, `需人工复核`.
- `反馈处理方式`: single select, fix strategy. Values: `局部修正` (default, only fix pointed issues), `整图重生` (may adjust composition/lighting).
- `反馈修正结果`: attachment, corrected main image(s). Separate from `首图结果` for comparison.
- `反馈修正结果_场景图`: attachment, corrected scene image(s). Separate from `场景图结果` for comparison.
- `反馈修正Prompt`: long text, exact prompt sent for the fix.
- `反馈质检结果`: single select, fix QA result (`通过`, `轻微问题可用`, `不通过`).
- `反馈质检问题`: long text, issues remaining after fix.
