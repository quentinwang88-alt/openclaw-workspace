# Token 自动更新 - 快速开始

## 3 分钟完成设置

### 1️⃣ 生成图标并安装扩展（1分钟）

```bash
# 生成图标
cd skills/inventory-query/browser-extension
pip install Pillow
python generate_icons.py
```

**安装到 Chrome/Edge：**
1. 打开 `chrome://extensions/`
2. 开启"开发者模式"
3. 点击"加载已解压的扩展程序"
4. 选择 `browser-extension` 目录

### 2️⃣ 启动本地服务（30秒）

```bash
cd skills/inventory-query
python token_manager.py server
```

看到这个就成功了：
```
🚀 Token 接收服务已启动
📡 监听地址: http://localhost:8765
```

### 3️⃣ 测试同步（30秒）

1. 浏览器访问 https://www.bigseller.pro 并登录
2. 点击扩展图标
3. 点击"立即同步"
4. 看到 ✅ 同步成功

## 完成！

现在你可以：
- ✅ 正常使用浏览器访问 BigSeller
- ✅ Token 会自动保持最新
- ✅ 无需手动更新配置

## 日常使用

**只需要做两件事：**

1. **启动服务**（开机后运行一次）
   ```bash
   cd skills/inventory-query
   python token_manager.py server
   ```

2. **正常使用浏览器**
   - 访问 BigSeller
   - 刷新页面
   - 重新登录
   - Token 自动同步 ✨

## 后台运行（可选）

如果想让服务一直运行：

```bash
# macOS/Linux
cd skills/inventory-query
nohup python token_receiver.py > token_receiver.log 2>&1 &

# 查看日志
tail -f token_receiver.log
```

## 验证是否工作

```bash
# 查看当前 Token 状态
python token_manager.py status

# 应该显示：
# ✅ Token 有效
#    过期时间: 2026-03-28 19:30:45
#    剩余天数: 20 天
```

## 遇到问题？

查看完整文档：[`TOKEN_AUTO_UPDATE_GUIDE.md`](TOKEN_AUTO_UPDATE_GUIDE.md)

常见问题：
- 扩展无法连接 → 确认服务正在运行
- 未找到 Token → 确认已登录 BigSeller
- 同步成功但查询失败 → 尝试重新登录

## 工作原理

```
你刷新页面 → 扩展检测到 Token 变化 → 发送到本地服务 → 自动更新配置
```

就这么简单！🎉
