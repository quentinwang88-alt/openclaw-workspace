# BigSeller Token 自动同步 - 修复说明

## 🐛 问题描述

用户点击浏览器扩展的"立即同步"按钮时，提示：
```
❌ 同步失败: 未知错误
```

## 🔍 根本原因

本地 Token 接收服务 ([`token_receiver.py`](token_receiver.py)) 未运行，导致浏览器扩展无法连接到 `http://localhost:8765/update-token`

## ✅ 已修复的问题

### 1. 改进错误提示信息

**修改文件**: 
- [`browser-extension/background.js`](browser-extension/background.js)
- [`browser-extension/popup.js`](browser-extension/popup.js)

**改进内容**:
- ❌ 旧提示: "同步失败: 未知错误"
- ✅ 新提示: "无法连接到本地服务器 (localhost:8765)。请确保 token_receiver.py 正在运行"

### 2. 添加服务器状态检查功能

**修改文件**: 
- [`browser-extension/popup.html`](browser-extension/popup.html)
- [`browser-extension/popup.js`](browser-extension/popup.js)

**新增功能**:
- 新增"检查服务器"按钮
- 可以快速诊断本地服务器是否运行
- 提供明确的错误提示和解决方案

### 3. 创建快速启动脚本

**新增文件**: [`start_token_receiver.sh`](start_token_receiver.sh)

**使用方法**:
```bash
cd skills/inventory-query
./start_token_receiver.sh
```

### 4. 完善故障排查文档

**新增文件**: 
- [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) - 详细故障排查指南
- [`QUICK_FIX_SYNC_ERROR.md`](QUICK_FIX_SYNC_ERROR.md) - 快速修复指南

## 📝 代码变更详情

### background.js 变更

```javascript
// 旧代码
async function sendTokenToServer(token) {
  try {
    // ...
    return false;  // 只返回布尔值
  } catch (error) {
    console.error('❌ 无法连接到本地服务器:', error.message);
    return false;
  }
}

// 新代码
async function sendTokenToServer(token) {
  try {
    // ...
    return { success: true, result };  // 返回详细结果
  } catch (error) {
    return { 
      success: false, 
      error: `无法连接到本地服务器 (${CONFIG.localServerUrl})。请确保 token_receiver.py 正在运行。错误: ${error.message}` 
    };
  }
}
```

### popup.js 变更

```javascript
// 新增服务器健康检查
document.getElementById('check-server').addEventListener('click', async () => {
  try {
    const response = await fetch('http://localhost:8765/update-token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: 'test', timestamp: new Date().toISOString(), source: 'health-check' })
    });
    
    if (response.ok) {
      showStatus('✅ 服务器运行正常 (localhost:8765)', 'active');
    } else {
      showStatus('⚠️ 服务器响应异常: ' + response.status, 'inactive');
    }
  } catch (error) {
    showStatus('❌ 无法连接到服务器。请运行: ./start_token_receiver.sh', 'error');
  }
});
```

## 🚀 使用指南

### 第一次使用

1. **启动本地服务器**:
   ```bash
   cd ~/.openclaw/workspace/skills/inventory-query
   ./start_token_receiver.sh
   ```

2. **重新加载浏览器扩展**:
   - 打开 `chrome://extensions/`
   - 找到 "BigSeller Token Auto Sync"
   - 点击刷新按钮 🔄

3. **测试同步**:
   - 访问 https://www.bigseller.pro 并登录
   - 点击扩展图标
   - 点击"检查服务器"确认服务运行
   - 点击"立即同步"

### 日常使用

扩展会自动监控 Token 变化，但需要确保本地服务器始终运行。

**推荐**: 使用 macOS LaunchAgent 自动启动服务器（参考 [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md)）

## 🔧 故障排查

如果遇到问题，按以下顺序检查:

1. **检查服务器是否运行**: 点击扩展的"检查服务器"按钮
2. **查看详细错误**: 右键扩展图标 → 检查弹出窗口 → Console
3. **参考文档**: 
   - 快速修复: [`QUICK_FIX_SYNC_ERROR.md`](QUICK_FIX_SYNC_ERROR.md)
   - 详细排查: [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md)

## 📊 测试验证

### 测试场景 1: 服务器未运行
```
操作: 点击"立即同步"
预期: ❌ 无法连接到本地服务器 (localhost:8765)。请确保 token_receiver.py 正在运行
```

### 测试场景 2: 服务器运行正常
```
操作: 启动服务器 → 点击"立即同步"
预期: ✅ 同步成功
```

### 测试场景 3: 未登录 BigSeller
```
操作: 未登录状态下点击"立即同步"
预期: ❌ 未找到 Token。请先访问 https://www.bigseller.pro 并登录
```

## 📚 相关文档

- [`START_HERE.md`](START_HERE.md) - 完整安装指南
- [`TOKEN_AUTO_UPDATE_MACOS.md`](TOKEN_AUTO_UPDATE_MACOS.md) - macOS 自动化配置
- [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) - 故障排查指南
- [`QUICK_FIX_SYNC_ERROR.md`](QUICK_FIX_SYNC_ERROR.md) - 快速修复指南
