# 妙手 DOM 校准记录

真实页面联调记录。页面结构优先沉淀到 `config/selectors/*.yaml`；需要按表头语义解析的 SKU 表不得退化为屏幕坐标或 `nth-child`。

| 页面区域 | Registry key | 首选稳定标识 | 已验证 |
|---|---|---|---|
| 登录状态 | `login_form` | data-testid / password input | 否 |
| 商品搜索 | `product_search_input` | 搜索区的组合筛选控件；仍需补稳定字段 | 部分 |
| 商品结果 | `product_result_rows` | `.pro-virtual-table__row` + 行内货源 ID | 是 |
| 编辑入口 | `product_row_edit_button` | 行内精确文本“编辑” | 是 |
| 编辑页就绪 | `editor_ready` | 编辑层内精确文本“保存并发布” | 是 |
| 发布店铺 | `shop_selector`, `shop_option` | 店铺精确文本 `LikeU shop`，店铺 ID `5159032` | 是 |
| SKU 采购价 | `sku_purchase_price` | SKU 行中按表头“货源价格”映射列 | 是（需实现语义映射） |
| SKU 售价 | `sku_sale_price` | SKU 行中按表头“本地展示价”映射列 | 是（需实现语义映射） |
| 仓库库存 | `stock_by_warehouse`, `stock_inputs` | 主表库存只读；逐 SKU 点齿轮进入“仓库及库存设置” | 是（现 handler 待改） |
| 仓库 | `warehouse_selector`, `warehouse_option` | 精确文本 `The Chinese mainland Pickup Warehouse` | 是（现 handler 待改） |
| 物流参数 | `weight_input` 等 | SKU 重量列 + “物流信息”中的包裹重量/长宽高 | 是（现 handler 待改） |
| 一键翻译 | `translate_button`, `translation_success` | 点击“一键翻译”后必须再选精确文本“泰语” | 是 |
| 详情图翻译 | 文本“批量翻译/处理图片” | 默认全选；打开“图片翻译”后取消“不翻译商品上的文字” | 是 |
| 主图翻译 | `.product-picture-list` 内“图片翻译” | 目标语言按市场选择，等待“翻译结果预览” | 是 |
| 尺码图翻译 | `.size-chart-box` 内“图片翻译” | 可信尺码图上传后翻译，禁止上传中文原图直接发布 | 是 |
| 发布弹窗 | `publish_dialog`, `market_option` | “发布产品”弹层；店铺 checkbox value `5159032`；“泰国站” | 是 |
| 发布验证 | `published_status`, `platform_product_id` | 发布记录中的“发布成功”与产品 ID | 是 |

## 2026-08-11 单品校准结果

- 页面：`/common_collect_box/items`（公用采集箱）与 `/tiktok/collect_box/items`（TikTok 快速上货）。
- 货源 ID：`975683523984`；公用采集箱 ID：`3887278816`。
- 店铺：`LikeU shop`；国家：泰国；自动匹配类目：`女士上装 > 女士夹克与风衣`。
- SKU 行为 `.pro-virtual-table__row-body`；本次规格为 `S / M / L / XL`。
- SKU 列顺序实际为：选择、颜色、尺码、货源价格、本地展示价、库存、平台 SKU、重量、预售类型、操作。生产实现应从表头映射列，不能固化索引。
- 库存输入不是主表可编辑框。每个 SKU 的库存齿轮会打开 `aria-label="仓库及库存设置"` 的弹层，需要逐 SKU 应用仓库与库存。
- 泰国目标仓库显示名为 `The Chinese mainland Pickup Warehouse`；同层还存在 `泰国-极兔仓` 与 `yacang-th`，不得选列表第一项。
- 一键翻译实际为二步操作：打开菜单，再选择 `泰语`；对应翻译与属性匹配接口均返回 200。
- 服装类目要求尺码图。本店暂无尺码模板；可通过“使用来源图片”选择 1688 详情中的商品自带尺码表。
- `保存修改` 调用 `saveShopCollectItemInfo` 返回 200 并提示“保存成功”。
- 用户明确授权后，发布任务接口返回 200；发布记录最终显示“发布成功”，TikTok 产品 ID 为 `1736979963960461306`。

详细证据见 [TEST_REPORT_2026-08-11_LIKEU_975683523984.md](./TEST_REPORT_2026-08-11_LIKEU_975683523984.md)。

禁止项：`nth-child`、绝对 XPath、屏幕坐标、依赖列表默认第一条、固定 `sleep(10)`。
