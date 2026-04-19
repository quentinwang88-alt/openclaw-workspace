# 安全配置说明

## ⚠️ 重要：敏感信息配置

本项目的 `skills/inventory-query/config/api_config.json` 文件包含敏感的 API 认证信息，已被 `.gitignore` 排除，不会提交到 Git。

### 首次设置步骤

1. **复制示例配置文件：**
   ```bash
   cp skills/inventory-query/config/api_config.example.json \
      skills/inventory-query/config/api_config.json
   ```

2. **获取你的认证信息：**
   - 在浏览器中登录 BigSeller
   - 打开开发者工具（F12）→ Network 标签
   - 刷新页面，找到任意 API 请求
   - 复制请求头中的 `Cookie` 值

3. **更新配置文件：**
   编辑 `skills/inventory-query/config/api_config.json`：
   ```json
   {
     "api": {
       "headers": {
         "Cookie": "粘贴你复制的完整 Cookie 字符串"
       },
       "payload_template": {
         "warehouseIds": [你的仓库ID]
       }
     }
   }
   ```

4. **测试连接：**
   ```bash
   cd skills/inventory-query
   python inventory_api.py
   ```

### 安全提醒

- ❌ **永远不要**将 `api_config.json` 提交到 Git
- ❌ **永远不要**在公开场合分享你的 Cookie 或 Token
- ✅ 定期更新你的认证信息（Cookie 会过期）
- ✅ 如果不小心泄露，立即在 BigSeller 后台退出登录重新登录

### 文件说明

- `api_config.json` - 你的实际配置（**已被 .gitignore 排除**）
- `api_config.example.json` - 示例配置模板（可以提交）

---

详细的 API 发现和配置指南请参考：
- `skills/inventory-query/docs/API_DISCOVERY_GUIDE.md`
- `skills/inventory-query/docs/API_QUICKSTART.md`
