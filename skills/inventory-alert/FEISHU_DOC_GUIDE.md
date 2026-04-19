# 飞书电子表格功能配置指南

## 功能说明

库存预警 skill 支持自动创建飞书电子表格，包含详细的预警信息：

- SKU编码
- SKU名称
- SKU图片
- 当前可用库存
- 日均销量
- 预计可售天数
- 建议采购数量（日均销量 × 15天）

## 配置步骤

### 1. 创建飞书应用

1. 访问 [飞书开放平台](https://open.feishu.cn/)
2. 创建企业自建应用
3. 获取 `App ID` 和 `App Secret`

### 2. 配置应用权限

在应用管理后台，添加以下权限：

- `sheets:spreadsheet` - 创建、编辑电子表格（必需）
- `drive:drive` - 访问云空间（可选，用于指定文件夹）

### 3. 获取文件夹 Token（可选）

如果想将文档创建到指定文件夹：

1. 在飞书云空间创建一个文件夹
2. 打开文件夹，从 URL 中获取 folder_token
   - URL 格式：`https://xxx.feishu.cn/drive/folder/{folder_token}`

### 4. 配置文件

编辑 [`config/alert_config.json`](config/alert_config.json)：

```json
{
  "threshold_days": 10,
  "feishu": {
    "enabled": true,
    "user_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_USER_WEBHOOK",
    "group_webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_GROUP_WEBHOOK",
    "app_id": "cli_xxxxxxxxxx",
    "app_secret": "xxxxxxxxxxxxxx",
    "create_doc": true,
    "doc_folder_token": "fldcnxxxxxxxxxx"
  }
}
```

参数说明：
- `app_id`: 飞书应用的 App ID
- `app_secret`: 飞书应用的 App Secret
- `create_doc`: 是否创建飞书文档（true/false）
- `doc_folder_token`: 文档存放的文件夹 Token（可选，留空则创建到根目录）

## 使用方法

配置完成后，运行库存预警时会自动创建飞书文档：

```bash
python3 alert.py
```

输出示例：

```
开始检查库存预警...
⚠️ 库存预警通知

发现 43 个 SKU 预计可售天数低于 10 天：
...

✓ 已发送通知给用户
✓ 已发送通知到群组
✓ 已创建飞书表格: https://xxx.feishu.cn/sheets/xxxxxxxxxx
```

## 表格内容示例

创建的飞书电子表格包含以下内容：

**第1行（标题）**：库存预警报告 - 2026-03-07 09:30

**第2行（基本信息）**：检查时间: 2026-03-07 09:30:00  |  预警数量: 43 个 SKU  |  预警阈值: 10 天

**第4行（表头）**：
| SKU编码 | SKU名称 | SKU图片 | 当前可用库存 | 日均销量 | 预计可售天数 | 建议采购数量 |

**第5行起（数据）**：
| WWJ001 | 产品名称 | https://... | 0 | 19.70 | 0 | 296 |
| pwj014 | 产品名称 | https://... | 0 | 3.69 | 0 | 55 |
| ... | ... | ... | ... | ... | ... | ... |

## 故障排查

### 问题：无法创建表格

1. 检查 `app_id` 和 `app_secret` 是否正确
2. 确认应用权限已配置（`sheets:spreadsheet`）
3. 检查应用是否已启用
4. 查看控制台输出的错误信息

### 问题：表格创建成功但没有数据

1. 检查控制台是否有"写入表格数据失败"的错误信息
2. 确认应用有写入权限
3. 查看返回的错误码和消息

### 问题：表格创建到了错误的位置

检查 `doc_folder_token` 是否正确，或者留空让表格创建到根目录。

## 注意事项

1. **权限管理**：确保应用有足够的权限创建和编辑文档
2. **配额限制**：飞书 API 有调用频率限制，避免频繁创建文档
3. **文档管理**：定期清理旧的预警文档，避免占用过多空间
4. **安全性**：`app_secret` 需要保密，不要提交到版本控制

## 高级功能

### 自定义表格格式

如果需要自定义表格格式，可以修改 [`alert.py`](alert.py) 中的 [`create_feishu_doc()`](alert.py:381) 和 [`_write_sheet_data()`](alert.py:444) 方法。

### 添加图表

可以使用飞书电子表格 API 添加图表，展示库存趋势等数据可视化内容。

### 分享表格

创建表格后，可以通过飞书 API 设置表格的分享权限，自动分享给相关人员。

## 相关文档

- [飞书开放平台文档](https://open.feishu.cn/document/)
- [飞书电子表格 API](https://open.feishu.cn/document/ukTMukTMukTM/uUDN04SN0QjL1QDN/sheets-v3/spreadsheet)
- [飞书应用权限](https://open.feishu.cn/document/ukTMukTMukTM/uQjN3QjL0YzN04CN2cDN)
