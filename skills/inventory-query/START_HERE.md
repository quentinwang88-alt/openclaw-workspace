# 🚀 立即开始 - 3步完成设置

## 第一步：确认扩展文件已准备好

打开终端，运行：

```bash
ls /Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension/
```

你应该看到这些文件：
```
✅ manifest.json
✅ background.js
✅ popup.html
✅ popup.js
✅ icon16.png
✅ icon48.png
✅ icon128.png
```

如果缺少图标文件（icon*.png），运行：
```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension
python3 generate_icons.py
```

## 第二步：安装浏览器扩展（1分钟）

### Chrome 浏览器：

1. **打开扩展管理页面**
   - 在地址栏输入：`chrome://extensions/`
   - 或者：菜单 → 更多工具 → 扩展程序

2. **开启开发者模式**
   - 右上角找到「开发者模式」开关
   - 点击开启（变成蓝色）

3. **加载扩展**
   - 点击左上角「加载已解压的扩展程序」按钮
   - 在弹出的文件选择器中，按 `Cmd+Shift+G` 输入路径：
     ```
     /Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension
     ```
   - 点击「选择」

4. **确认安装成功**
   - 扩展列表中出现「BigSeller Token Auto Sync」
   - 浏览器工具栏出现蓝色 T 字母图标

### Edge 浏览器：

1. 地址栏输入：`edge://extensions/`
2. 左下角开启「开发人员模式」
3. 点击「加载解压缩的扩展」
4. 选择目录：`/Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension`

## 第三步：启动自动接收服务（30秒）

打开终端，运行：

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

**保持这个终端窗口打开！**

## 第四步：测试同步（30秒）

1. **访问 BigSeller**
   - 打开浏览器，访问：https://www.bigseller.pro
   - 登录你的账号

2. **打开扩展**
   - 点击浏览器工具栏的蓝色 T 图标
   - 会弹出一个小窗口

3. **立即同步**
   - 点击「立即同步」按钮
   - 看到 ✅ 同步成功

4. **查看服务日志**
   - 回到运行服务的终端窗口
   - 应该看到：
     ```
     ✅ Token 更新成功
     📝 配置已更新，备份保存至: api_config.backup.20260308_194523.json
     ```

## 🎉 完成！

现在你可以：
- ✅ 正常使用浏览器访问 BigSeller
- ✅ 刷新页面时 Token 自动更新
- ✅ 重新登录时 Token 自动更新
- ✅ 无需手动维护配置

## 📸 截图指南

### 如何找到扩展目录

在 Finder 中：
1. 按 `Cmd+Shift+G`
2. 输入：`/Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension`
3. 按回车

你会看到这个目录：

```
browser-extension/
├── manifest.json       ← 扩展配置文件
├── background.js       ← 后台脚本
├── popup.html          ← 弹窗界面
├── popup.js            ← 界面逻辑
├── icon16.png          ← 小图标
├── icon48.png          ← 中图标
└── icon128.png         ← 大图标
```

### Chrome 扩展管理页面

![Chrome Extensions](https://i.imgur.com/example.png)

1. 地址栏输入 `chrome://extensions/`
2. 右上角开启「开发者模式」
3. 左上角点击「加载已解压的扩展程序」

## 常见问题

### Q: 找不到 browser-extension 目录

**A:** 在终端运行：
```bash
open /Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension
```
这会在 Finder 中打开目录。

### Q: 扩展安装后没有图标

**A:** 生成图标：
```bash
cd /Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension
python3 generate_icons.py
```
然后在 `chrome://extensions/` 页面点击扩展的刷新按钮。

### Q: 点击「加载已解压的扩展程序」后不知道选什么

**A:** 
1. 在文件选择器中按 `Cmd+Shift+G`
2. 粘贴路径：`/Users/likeu3/.openclaw/workspace/skills/inventory-query/browser-extension`
3. 按回车
4. 点击「选择」按钮

### Q: 服务启动失败

**A:** 检查端口是否被占用：
```bash
lsof -i :8765
```
如果有输出，说明端口被占用，可以换个端口：
```bash
python3 token_manager.py server 9000
```
同时需要修改扩展的 `background.js` 中的端口号。

## 验证安装

运行这个命令检查 Token 状态：
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

## 下一步

- 后台运行服务：查看 [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md#后台运行推荐)
- 开机自启动：查看 [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md#开机自启动可选)
- 快捷命令：查看 [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md#快捷命令)

## 需要帮助？

如果遇到问题，查看完整故障排查指南：
[`TOKEN_AUTO_UPDATE_GUIDE.md#故障排查`](TOKEN_AUTO_UPDATE_GUIDE.md#故障排查)
