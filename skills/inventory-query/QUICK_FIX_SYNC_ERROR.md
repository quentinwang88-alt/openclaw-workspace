# 🚨 快速修复: "同步失败: 未知错误"

## 问题原因
本地 Token 接收服务未运行，浏览器扩展无法连接到 `http://localhost:8765`

## ✅ 解决方案（3步）

### 1️⃣ 启动本地服务器

打开终端，运行:

```bash
cd ~/.openclaw/workspace/skills/inventory-query
./start_token_receiver.sh
```

或者:

```bash
cd ~/.openclaw/workspace/skills/inventory-query
python3 token_receiver.py
```

你应该看到:
```
🚀 Token 接收服务已启动
📡 监听地址: http://localhost:8765
📝 配置文件: ...
⏸️  按 Ctrl+C 停止服务
```

### 2️⃣ 验证服务运行

在浏览器扩展中点击 **"检查服务器"** 按钮

或在终端运行:
```bash
lsof -i :8765
```

应该看到 Python 进程占用端口 8765

### 3️⃣ 重新同步 Token

1. 访问 https://www.bigseller.pro 并登录
2. 点击浏览器扩展图标
3. 点击 **"立即同步"** 按钮
4. 应该看到 "✅ 同步成功"

---

## 🔧 如果还是失败

### 检查端口是否被占用
```bash
lsof -i :8765
```

如果端口被其他程序占用:
```bash
kill -9 $(lsof -t -i:8765)
```

### 检查配置文件权限
```bash
ls -la ~/.openclaw/workspace/skills/inventory-query/config/api_config.json
chmod 644 ~/.openclaw/workspace/skills/inventory-query/config/api_config.json
```

### 重新加载浏览器扩展
1. 打开 `chrome://extensions/`
2. 找到 "BigSeller Token Auto Sync"
3. 点击刷新按钮 🔄

---

## 📚 更多帮助

详细故障排查指南: [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md)
