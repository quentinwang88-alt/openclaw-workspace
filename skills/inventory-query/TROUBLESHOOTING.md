# BigSeller Token 自动同步 - 故障排查指南

## 常见错误及解决方案

### ❌ 错误: "同步失败: 未知错误"

**原因**: 本地 Token 接收服务未启动

**解决方案**:
```bash
cd skills/inventory-query
./start_token_receiver.sh
```

或手动启动:
```bash
cd skills/inventory-query
python3 token_receiver.py
```

**验证服务是否运行**:
```bash
lsof -i :8765
# 或
curl http://localhost:8765/update-token
```

---

### ❌ 错误: "无法连接到本地服务器 (localhost:8765)"

**原因**: Token 接收服务未运行或端口被占用

**解决步骤**:

1. **检查服务是否运行**:
   ```bash
   lsof -i :8765
   ```

2. **如果端口被占用，杀死进程**:
   ```bash
   kill -9 $(lsof -t -i:8765)
   ```

3. **重新启动服务**:
   ```bash
   cd skills/inventory-query
   ./start_token_receiver.sh
   ```

---

### ❌ 错误: "未找到 Token"

**原因**: 浏览器中没有 BigSeller 的登录 Cookie

**解决方案**:

1. 访问 https://www.bigseller.pro
2. 登录你的账号
3. 确保登录成功后，再点击扩展的"立即同步"按钮

**验证 Cookie 是否存在**:
- 按 F12 打开开发者工具
- Application → Cookies → bigseller.pro
- 查找 `muc_token` Cookie

---

### ❌ 错误: "服务器返回错误 500"

**原因**: Token 接收服务内部错误

**解决步骤**:

1. **查看服务器日志**:
   检查运行 `token_receiver.py` 的终端输出

2. **检查配置文件**:
   ```bash
   ls -la skills/inventory-query/config/api_config.json
   ```

3. **确保配置文件可写**:
   ```bash
   chmod 644 skills/inventory-query/config/api_config.json
   ```

4. **重启服务**:
   - 按 Ctrl+C 停止服务
   - 重新运行 `./start_token_receiver.sh`

---

### 🔧 扩展未自动监控

**原因**: 扩展后台服务未启动或被浏览器暂停

**解决方案**:

1. **重新加载扩展**:
   - 打开 `chrome://extensions/`
   - 找到 "BigSeller Token Auto Sync"
   - 点击刷新按钮 🔄

2. **检查扩展权限**:
   - 确保扩展有 `cookies` 和 `storage` 权限
   - 确保 `*://*.bigseller.pro/*` 在 host_permissions 中

3. **查看扩展日志**:
   - 右键点击扩展图标
   - 选择"检查弹出窗口"
   - 查看 Console 标签的错误信息

---

### 🔧 Token 同步后仍然提示过期

**原因**: 配置文件更新延迟或 Token 格式错误

**解决步骤**:

1. **手动验证 Token**:
   ```bash
   cd skills/inventory-query
   python3 -c "
   import json
   with open('config/api_config.json') as f:
       config = json.load(f)
   cookie = config['api']['headers']['cookie']
   print('muc_token' in cookie)
   "
   ```

2. **检查 Token 格式**:
   - Token 应该是 JWT 格式 (eyJ 开头)
   - 确保 Cookie 字符串中包含 `muc_token=eyJ...`

3. **重新获取 Token**:
   - 退出 BigSeller 账号
   - 重新登录
   - 点击扩展的"立即同步"

---

## 完整测试流程

### 1. 启动本地服务
```bash
cd skills/inventory-query
./start_token_receiver.sh
```

应该看到:
```
🚀 Token 接收服务已启动
📡 监听地址: http://localhost:8765
📝 配置文件: /path/to/config/api_config.json
⏸️  按 Ctrl+C 停止服务
```

### 2. 测试服务连接
在另一个终端:
```bash
curl -X POST http://localhost:8765/update-token \
  -H "Content-Type: application/json" \
  -d '{"token":"test123","timestamp":"2024-01-01T00:00:00Z","source":"test"}'
```

应该返回:
```json
{"success": true, "message": "Token 更新成功", "timestamp": "..."}
```

### 3. 测试浏览器扩展

1. 访问 https://www.bigseller.pro 并登录
2. 点击扩展图标
3. 点击"立即同步"按钮
4. 应该看到 "✅ 同步成功"

### 4. 验证配置更新

```bash
cd skills/inventory-query
tail -n 20 config/api_config.json
```

检查 `muc_token` 是否已更新

---

## 日志位置

- **扩展日志**: Chrome DevTools → Console (右键扩展图标 → 检查弹出窗口)
- **后台服务日志**: 运行 `token_receiver.py` 的终端输出
- **配置备份**: `skills/inventory-query/config/api_config.backup.*.json`

---

## 需要帮助?

如果以上方法都无法解决问题:

1. 收集以下信息:
   - 扩展版本
   - 浏览器版本
   - 错误截图
   - 服务器日志
   - 扩展 Console 日志

2. 检查相关文档:
   - [`START_HERE.md`](START_HERE.md) - 完整安装指南
   - [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md) - macOS 自动化配置
