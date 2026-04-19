# 🔧 快速修复指南 - Cookie 更新

## 问题
API 返回错误（状态码: 2001）- Cookie 已过期

## 解决方案（5 分钟）

### 步骤 1: 登录系统
打开浏览器，访问：https://www.bigseller.pro

### 步骤 2: 打开开发者工具
- Windows/Linux: 按 `F12`
- Mac: 按 `Cmd + Option + I`

### 步骤 3: 切换到 Network 标签
点击开发者工具顶部的 **Network** 标签

### 步骤 4: 访问库存页面
在网站中导航到库存管理页面

### 步骤 5: 找到 API 请求
1. 在 Network 列表中找到 `pageList.json` 请求
2. 点击该请求
3. 在右侧面板中找到 **Request Headers** 部分

### 步骤 6: 复制 Cookie
1. 找到 `Cookie:` 这一行
2. 复制整个 Cookie 值（通常很长）
3. 示例格式：
   ```
   JSESSIONID=xxx; token=yyy; user_id=zzz; ...
   ```

### 步骤 7: 更新配置文件
编辑 `skills/inventory-query/config/api_config.json`：

```json
{
  "api": {
    "headers": {
      "Cookie": "这里粘贴刚才复制的完整 Cookie"
    }
  }
}
```

### 步骤 8: 验证修复
运行测试：
```bash
cd skills/inventory-query
python3 test_api.py
```

## 预期结果

成功后应该看到：
```
✓ 查询成功！
  SKU: BU0010
  可用库存: 112
  总库存: 112
```

## 注意事项

- Cookie 会定期过期（通常几天到几周）
- 如果再次出现 2001 错误，重复上述步骤
- 不要分享你的 Cookie，它包含登录凭证

## 需要帮助？

查看完整诊断报告：[`ERROR_DIAGNOSIS.md`](ERROR_DIAGNOSIS.md)
