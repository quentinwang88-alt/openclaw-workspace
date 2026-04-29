# Phase1 Rules

## 核心流程

1. 读取启用的表配置
2. 按表配置字段映射生成 `CandidateTask`
3. 规则预检查
4. 人工类目优先，缺失时再走 Hermes 类目识别
5. 仅当最终类目为 `发饰` 或 `轻上装` 时进入深分析
6. 写回状态、识别结果、短分析结论

## 状态流转

- `待处理`
- `信息不足`
- `待人工确认类目`
- `当前类目不支持`
- `已完成分析`
- `分析失败`

## 保守原则

- 没有 `product_images` 时直接写回 `信息不足`
- 人工类目存在但不在支持范围内时直接写回 `当前类目不支持`
- Hermes 类目识别为 `其他`、`无法判断` 或 `low` 时写回 `待人工确认类目`
- Hermes 返回非 JSON、缺字段、枚举非法、图片准备失败、远程调用失败时写回 `分析失败`

## 配置补充说明

phase1 原方案里的 `table_id` 是逻辑配置 ID。为了实际连接飞书表，本实现额外支持：

```json
{
  "source": {
    "feishu_url": "https://xxx.feishu.cn/base/APPTOKEN?table=tblxxx&view=vewxxx"
  }
}
```

也支持：

```json
{
  "source": {
    "app_token": "APPTOKEN",
    "bitable_table_id": "tblxxx"
  }
}
```
