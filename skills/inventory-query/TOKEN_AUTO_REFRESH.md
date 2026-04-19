# BigSeller API Token 自动刷新方案

## 问题分析

BigSeller API 使用 JWT Token (`muc_token`) 进行认证，该 Token 会定期过期（通常 20 天左右）。

**错误代码：401006** - Token 认证失败

## 长期解决方案

### 方案 1：自动提取 Token（推荐）⭐

从浏览器自动提取最新的 Token，无需手动更新。

#### 实现步骤：

1. **安装浏览器 Cookie 提取工具**
```bash
pip install browser-cookie3
```

2. **创建 Token 自动提取脚本**（见 `token_manager.py`）

3. **修改 API 配置**，使用动态 Token 加载

#### 优点：
- ✅ 完全自动化，无需人工干预
- ✅ 始终使用最新的 Token
- ✅ 支持多浏览器（Chrome、Firefox、Safari）

#### 缺点：
- ⚠️ 需要保持浏览器登录状态
- ⚠️ 首次需要配置浏览器选择

---

### 方案 2：Token 过期检测 + 提醒

检测 Token 是否即将过期，提前提醒更新。

#### 实现步骤：

1. **解析 JWT Token 获取过期时间**
2. **在每次 API 调用前检查**
3. **过期前 3 天发送提醒**（通过飞书/邮件）

#### 优点：
- ✅ 简单可靠
- ✅ 不依赖浏览器
- ✅ 提前预警，避免突然失效

#### 缺点：
- ⚠️ 仍需手动更新 Token
- ⚠️ 需要配置提醒渠道

---

### 方案 3：使用 BigSeller 官方 API（如果有）

如果 BigSeller 提供官方 API 和 API Key，使用官方认证方式。

#### 检查方法：
1. 登录 BigSeller 后台
2. 查找"开放平台"、"API 管理"、"开发者中心"等入口
3. 申请 API Key 和 Secret

#### 优点：
- ✅ 官方支持，稳定可靠
- ✅ Token 自动刷新机制
- ✅ 更长的有效期

---

## 推荐实施方案

**组合方案：方案 1 + 方案 2**

1. **主要使用方案 1**（自动提取）作为日常方案
2. **方案 2 作为备用**（过期检测）防止意外情况
3. **定期检查方案 3**（官方 API）是否可用

## 快速修复（临时）

如果需要立即恢复服务：

1. **打开浏览器，登录 BigSeller**
2. **打开开发者工具**（F12）
3. **访问库存页面**：https://www.bigseller.pro/web/inventory/index.htm
4. **查看 Network 标签**，找到任意 API 请求
5. **复制 Cookie 中的 `muc_token`**
6. **更新配置文件**：
```bash
# 编辑配置
nano skills/inventory-query/config/api_config.json

# 找到 cookie 字段，替换 muc_token 的值
```

## 下一步

选择你想实施的方案，我可以帮你：
1. 实现自动 Token 提取（方案 1）
2. 添加过期检测和提醒（方案 2）
3. 调研 BigSeller 官方 API（方案 3）
