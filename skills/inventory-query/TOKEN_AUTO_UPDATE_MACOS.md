# Token 自动更新 - macOS 快速开始

## 🍎 macOS 专用指南

### 第一步：运行安装脚本（1分钟）

```bash
cd skills/inventory-query
./install_auto_update.sh
```

脚本会自动：
- ✅ 检查 Python 环境
- ✅ 安装 Pillow 依赖
- ✅ 生成浏览器扩展图标

### 第二步：安装浏览器扩展（1分钟）

#### Chrome 浏览器：

1. 打开 Chrome，地址栏输入：`chrome://extensions/`
2. 右上角开启「开发者模式」
3. 点击左上角「加载已解压的扩展程序」
4. 选择目录：`/Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension`
5. 扩展安装完成，工具栏会出现图标

#### Edge 浏览器：

1. 打开 Edge，地址栏输入：`edge://extensions/`
2. 左下角开启「开发人员模式」
3. 点击「加载解压缩的扩展」
4. 选择目录：`/Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension`

### 第三步：启动自动接收服务（30秒）

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
python3 token_manager.py server
```

看到这个就成功了：
```
🚀 Token 接收服务已启动
📡 监听地址: http://localhost:8765
📝 配置文件: /Users/likeu3/.openclaw/workspace/skills/inventory-query/config/api_config.json
⏸️  按 Ctrl+C 停止服务
```

### 第四步：测试同步（30秒）

1. 浏览器访问 https://www.bigseller.pro 并登录
2. 点击浏览器工具栏的扩展图标（蓝色 T 字母）
3. 点击「立即同步」按钮
4. 看到 ✅ 同步成功

## 🎉 完成！

现在你可以：
- ✅ 正常使用浏览器访问 BigSeller
- ✅ 刷新页面时 Token 自动更新
- ✅ 重新登录时 Token 自动更新
- ✅ 无需手动维护配置

## 日常使用

### 启动服务（开机后运行一次）

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
python3 token_manager.py server
```

### 后台运行（推荐）

如果想让服务一直在后台运行：

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
nohup python3 token_receiver.py > token_receiver.log 2>&1 &
```

查看日志：
```bash
tail -f /Users/likeu3/.openclaw/workspace/skills/inventory-query/token_receiver.log
```

停止服务：
```bash
ps aux | grep token_receiver
kill <PID>
```

### 开机自启动（可选）

创建 launchd 配置文件：

```bash
cat > ~/Library/LaunchAgents/com.bigseller.token-receiver.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.bigseller.token-receiver</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Users/likeu3/.openclaw/workspace/skills/inventory-query/token_receiver.py</string>
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
EOF
```

加载服务：
```bash
launchctl load ~/Library/LaunchAgents/com.bigseller.token-receiver.plist
launchctl start com.bigseller.token-receiver
```

查看状态：
```bash
launchctl list | grep bigseller
```

停止并卸载：
```bash
launchctl stop com.bigseller.token-receiver
launchctl unload ~/Library/LaunchAgents/com.bigseller.token-receiver.plist
```

## 验证是否工作

```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
python3 token_manager.py status
```

应该显示：
```
📊 当前 Token 状态:
✅ Token 有效
   过期时间: 2026-03-28 19:30:45
   剩余天数: 20 天
```

## 常见问题

### Q: 扩展无法连接到本地服务

**解决：**
```bash
# 检查服务是否运行
lsof -i :8765

# 如果没有输出，说明服务未启动，运行：
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query
python3 token_manager.py server
```

### Q: 扩展未找到 Token

**解决：**
1. 确认已在浏览器中登录 BigSeller
2. 访问任意 BigSeller 页面（如库存页面）
3. 按 `Cmd+Option+I` 打开开发者工具
4. Application → Cookies → bigseller.pro
5. 查找 `muc_token`

### Q: 如何查看同步日志

**解决：**
```bash
# 如果使用 nohup 后台运行
tail -f /Users/likeu3/.openclaw/workspace/skills/inventory-query/token_receiver.log

# 如果使用 launchd
tail -f /tmp/token-receiver.log
```

## macOS 特定提示

1. **首次运行可能需要授权**
   - 系统可能提示「无法验证开发者」
   - 前往「系统偏好设置」→「安全性与隐私」→「仍要打开」

2. **防火墙设置**
   - 如果防火墙阻止连接，允许 Python 接受传入连接

3. **使用 Homebrew Python**
   - 如果使用 Homebrew 安装的 Python，路径可能是 `/opt/homebrew/bin/python3`
   - 相应修改 launchd 配置中的路径

## 快捷命令

保存这些命令到 `~/.zshrc` 或 `~/.bash_profile`：

```bash
# Token 服务管理
alias token-start='cd /Users/likeu3/.openclaw/workspace/skills/inventory-query && python3 token_manager.py server'
alias token-status='cd /Users/likeu3/.openclaw/workspace/skills/inventory-query && python3 token_manager.py status'
alias token-log='tail -f /Users/likeu3/.openclaw/workspace/skills/inventory-query/token_receiver.log'
```

重新加载配置：
```bash
source ~/.zshrc  # 或 source ~/.bash_profile
```

之后就可以直接使用：
```bash
token-start   # 启动服务
token-status  # 查看状态
token-log     # 查看日志
```

## 工作原理

```
你在浏览器刷新 BigSeller 页面
         ↓
扩展检测到 muc_token Cookie 变化
         ↓
发送 HTTP POST 到 localhost:8765
         ↓
本地服务接收并更新 api_config.json
         ↓
自动备份旧配置
         ↓
完成！API 查询使用最新 Token
```

## 下一步

- 查看完整文档：[`TOKEN_AUTO_UPDATE_GUIDE.md`](TOKEN_AUTO_UPDATE_GUIDE.md)
- 了解实现细节：[`TOKEN_AUTO_UPDATE_IMPLEMENTATION.md`](TOKEN_AUTO_UPDATE_IMPLEMENTATION.md)
- 遇到问题查看故障排查：[`TOKEN_AUTO_UPDATE_GUIDE.md#故障排查`](TOKEN_AUTO_UPDATE_GUIDE.md#故障排查)
