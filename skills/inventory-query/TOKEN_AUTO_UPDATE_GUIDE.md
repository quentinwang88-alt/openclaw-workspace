# Token 自动更新完整方案

## 概述

通过浏览器扩展 + 本地服务的方式，实现 BigSeller Token 的自动监听和更新。当你在浏览器中刷新页面或重新登录时，Token 会自动同步到配置文件。

## 架构

```
浏览器 (BigSeller)
    ↓ Cookie 变化
浏览器扩展 (监听)
    ↓ HTTP POST
本地服务 (接收)
    ↓ 更新
配置文件 (api_config.json)
```

## 安装步骤

### 第一步：生成扩展图标

```bash
cd skills/inventory-query/browser-extension
pip install Pillow
python generate_icons.py
```

### 第二步：安装浏览器扩展

#### Chrome/Edge 安装：

1. 打开浏览器，访问 `chrome://extensions/`
2. 开启右上角的"开发者模式"
3. 点击"加载已解压的扩展程序"
4. 选择 `skills/inventory-query/browser-extension` 目录
5. 扩展安装完成，图标会出现在工具栏

#### Firefox 安装：

1. 打开浏览器，访问 `about:debugging#/runtime/this-firefox`
2. 点击"临时加载附加组件"
3. 选择 `browser-extension/manifest.json` 文件
4. 扩展安装完成

### 第三步：启动本地接收服务

```bash
cd skills/inventory-query

# 方法 1：使用 token_manager
python token_manager.py server

# 方法 2：直接启动服务
python token_receiver.py

# 自定义端口（默认 8765）
python token_manager.py server 9000
```

服务启动后会显示：
```
🚀 Token 接收服务已启动
📡 监听地址: http://localhost:8765
📝 配置文件: /path/to/api_config.json
⏸️  按 Ctrl+C 停止服务
```

### 第四步：测试同步

1. 在浏览器中访问 BigSeller：https://www.bigseller.pro
2. 登录你的账号
3. 点击浏览器工具栏的扩展图标
4. 点击"立即同步"按钮
5. 查看本地服务日志，应该显示：
   ```
   ✅ Token 更新成功
   📝 配置已更新，备份保存至: api_config.backup.20260308_193045.json
   ```

## 使用方式

### 自动模式（推荐）

扩展会自动监听 Token 变化：

- ✅ 每 5 秒检查一次 Cookie
- ✅ 检测到变化立即同步
- ✅ 浏览器刷新页面时自动捕获
- ✅ 重新登录时自动更新

**你只需要：**
1. 保持本地服务运行
2. 正常使用浏览器访问 BigSeller
3. Token 会自动保持最新

### 手动模式

如果需要立即同步：

1. 点击扩展图标
2. 点击"立即同步"按钮
3. 查看同步状态

## 扩展界面说明

点击扩展图标后会显示：

```
┌─────────────────────────┐
│  Token 自动同步          │
├─────────────────────────┤
│ 🟢 监控中 - Token 已同步 │
├─────────────────────────┤
│ 监控状态: 运行中         │
│ Token 状态: 已获取       │
│ 最后同步: 2026-03-08... │
├─────────────────────────┤
│ [立即同步]              │
│ [切换监控]              │
│ [打开配置]              │
└─────────────────────────┘
```

## 后台运行

### macOS/Linux

使用 `nohup` 后台运行：

```bash
cd skills/inventory-query
nohup python token_receiver.py > token_receiver.log 2>&1 &

# 查看日志
tail -f token_receiver.log

# 停止服务
ps aux | grep token_receiver
kill <PID>
```

### 使用 systemd（Linux）

创建服务文件 `/etc/systemd/system/token-receiver.service`：

```ini
[Unit]
Description=BigSeller Token Receiver
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/skills/inventory-query
ExecStart=/usr/bin/python3 token_receiver.py
Restart=always

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
sudo systemctl enable token-receiver
sudo systemctl start token-receiver
sudo systemctl status token-receiver
```

### 使用 launchd（macOS）

创建 `~/Library/LaunchAgents/com.bigseller.token-receiver.plist`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.bigseller.token-receiver</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/skills/inventory-query/token_receiver.py</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/token-receiver.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/token-receiver.error.log</string>
</dict>
</plist>
```

加载服务：
```bash
launchctl load ~/Library/LaunchAgents/com.bigseller.token-receiver.plist
launchctl start com.bigseller.token-receiver
```

## 配置备份

每次更新 Token 时，会自动创建配置备份：

```
config/
├── api_config.json                          # 当前配置
├── api_config.backup.20260308_193045.json  # 备份 1
├── api_config.backup.20260308_194523.json  # 备份 2
└── ...
```

如需恢复旧配置：
```bash
cp config/api_config.backup.20260308_193045.json config/api_config.json
```

## 故障排查

### 扩展无法连接到本地服务

**症状：** 点击"立即同步"显示"无法连接到本地服务器"

**解决：**
1. 确认本地服务正在运行：
   ```bash
   lsof -i :8765
   ```
2. 检查防火墙是否阻止了连接
3. 确认端口号配置正确（扩展和服务要一致）

### 扩展未找到 Token

**症状：** Token 状态显示"未找到"

**解决：**
1. 确认已在浏览器中登录 BigSeller
2. 访问任意 BigSeller 页面（如库存页面）
3. 检查浏览器 Cookie 中是否有 `muc_token`：
   - 按 F12 打开开发者工具
   - Application → Cookies → bigseller.pro
   - 查找 `muc_token`

### Token 同步成功但查询仍失败

**症状：** 扩展显示同步成功，但 API 查询返回 401

**解决：**
1. 检查配置文件是否真的更新了：
   ```bash
   cat config/api_config.json | grep muc_token
   ```
2. Token 可能已过期，尝试：
   - 退出 BigSeller 账号
   - 重新登录
   - 等待扩展自动同步

### 扩展图标不显示

**症状：** 扩展已安装但工具栏没有图标

**解决：**
1. 生成图标文件：
   ```bash
   cd browser-extension
   python generate_icons.py
   ```
2. 重新加载扩展：
   - Chrome: `chrome://extensions/` → 点击刷新按钮
   - Firefox: `about:debugging` → 重新加载

## 安全说明

1. **本地服务仅监听 localhost**，不会暴露到外网
2. **Token 仅在本机传输**，不会发送到任何第三方服务器
3. **配置文件自动备份**，防止意外覆盖
4. **建议定期检查备份文件**，清理过期备份

## 性能影响

- **浏览器扩展：** 每 5 秒检查一次 Cookie，CPU 占用 < 0.1%
- **本地服务：** 待机状态内存占用 < 10MB
- **网络流量：** 仅在 Token 变化时发送一次请求（< 1KB）

## 与其他方案对比

| 方案 | 自动化程度 | 实时性 | 复杂度 | 推荐度 |
|------|-----------|--------|--------|--------|
| **浏览器扩展 + 本地服务** | ⭐⭐⭐⭐⭐ | 5秒内 | 中 | ⭐⭐⭐⭐⭐ |
| 手动从浏览器提取 | ⭐⭐ | 手动 | 低 | ⭐⭐⭐ |
| 定时从浏览器读取 | ⭐⭐⭐⭐ | 分钟级 | 低 | ⭐⭐⭐⭐ |
| Token 过期提醒 | ⭐⭐ | 天级 | 低 | ⭐⭐ |

## 相关文件

- [`browser-extension/`](browser-extension/) - 浏览器扩展源码
- [`token_receiver.py`](token_receiver.py) - 本地接收服务
- [`token_manager.py`](token_manager.py) - Token 管理工具
- [`TOKEN_AUTO_REFRESH.md`](TOKEN_AUTO_REFRESH.md) - 其他方案说明

## 常见问题

**Q: 需要一直开着浏览器吗？**  
A: 不需要。服务可以一直运行，只有在你使用浏览器访问 BigSeller 时才会同步。

**Q: 可以在多台电脑上使用吗？**  
A: 可以。每台电脑独立安装扩展和服务即可。

**Q: Token 多久会过期？**  
A: 通常 20 天左右。使用本方案后无需关心过期时间。

**Q: 会影响浏览器性能吗？**  
A: 几乎没有影响。扩展非常轻量，只在后台定期检查 Cookie。

**Q: 可以同时监听多个网站的 Token 吗？**  
A: 当前版本仅支持 BigSeller。如需支持其他网站，需要修改扩展配置。

## 下一步优化

- [ ] 支持多网站 Token 监听
- [ ] 添加 Token 过期提醒
- [ ] 支持 Token 历史记录查看
- [ ] 添加 Web 管理界面
- [ ] 支持远程同步（加密传输）
