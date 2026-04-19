# 飞书多维表格命令问题诊断与修复

## 问题描述

程序中调用 `openclaw feishu-bitable` 命令失败，提示该命令不存在。

## 根本原因

**飞书插件提供的是 MCP 工具，而不是 CLI 命令。**

从插件加载日志可以看到：
```
[plugins] feishu_bitable: Registered bitable tools
```

这意味着飞书多维表格功能是作为 **MCP (Model Context Protocol) 工具** 注册的，只能在 OpenClaw 的 Agent 会话中通过工具调用使用，而不能作为独立的 CLI 命令执行。

## 受影响的文件

1. [`import_to_bitable.py`](import_to_bitable.py:64) - 使用了 `openclaw feishu-bitable create-record`
2. [`alert.py`](alert.py:401) - 使用了 `openclaw feishu-bitable list-records`
3. [`alert.py`](alert.py:563) - 使用了 `openclaw feishu-bitable delete-record`
4. [`alert.py`](alert.py:600) - 使用了 `openclaw feishu-bitable create-record`

## 解决方案

有两种方式可以解决这个问题：

### 方案 1：直接调用飞书 API（推荐）

不依赖 OpenClaw 命令，直接使用 Python 的 `requests` 库调用飞书开放平台 API。

**优点：**
- 独立运行，不依赖 OpenClaw Gateway
- 更快的响应速度
- 更容易调试和维护
- 可以在任何环境运行

**实现示例：**

```python
import requests
import json

class FeishuBitableAPI:
    """飞书多维表格 API 封装"""
    
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_url = "https://open.feishu.cn/open-apis"
        self._access_token = None
    
    def get_access_token(self) -> str:
        """获取 tenant_access_token"""
        if self._access_token:
            return self._access_token
        
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        response = requests.post(url, json=payload)
        data = response.json()
        
        if data.get("code") == 0:
            self._access_token = data["tenant_access_token"]
            return self._access_token
        else:
            raise Exception(f"获取 access_token 失败: {data}")
    
    def create_record(self, app_token: str, table_id: str, fields: dict) -> dict:
        """创建记录"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json"
        }
        payload = {"fields": fields}
        
        response = requests.post(url, headers=headers, json=payload)
        return response.json()
    
    def list_records(self, app_token: str, table_id: str, 
                     page_size: int = 100, page_token: str = None) -> dict:
        """列出记录"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}"
        }
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(url, headers=headers, params=params)
        return response.json()
    
    def delete_record(self, app_token: str, table_id: str, record_id: str) -> dict:
        """删除记录"""
        url = f"{self.base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}"
        }
        
        response = requests.delete(url, headers=headers)
        return response.json()
```

### 方案 2：通过 OpenClaw Gateway 调用 MCP 工具

需要启动 OpenClaw Gateway，然后通过 Agent 会话调用工具。

**缺点：**
- 需要 Gateway 运行
- 调用链路更复杂
- 不适合批量操作
- 需要处理会话管理

**不推荐用于批量数据导入场景。**

## 修复步骤

### 1. 创建飞书 API 封装类

创建 [`feishu_api.py`](feishu_api.py)（参考上面的实现示例）

### 2. 修改 [`alert.py`](alert.py)

将所有 `subprocess.run(["openclaw", "feishu-bitable", ...])` 调用替换为直接 API 调用：

```python
# 旧代码
cmd = ["openclaw", "feishu-bitable", "create-record", ...]
result = subprocess.run(cmd, ...)

# 新代码
from feishu_api import FeishuBitableAPI

api = FeishuBitableAPI(app_id, app_secret)
result = api.create_record(app_token, table_id, fields)
```

### 3. 修改 [`import_to_bitable.py`](import_to_bitable.py)

同样替换为直接 API 调用。

### 4. 更新配置文件

确保 [`config/alert_config.json`](config/alert_config.json) 包含必要的飞书应用凭证：

```json
{
  "feishu": {
    "app_id": "cli_xxxxxxxxxx",
    "app_secret": "xxxxxxxxxxxxxx",
    "bitable": {
      "app_token": "ItCzbDvR3aIXuss5UeUcko1SnUg",
      "table_id": "tblNIGOGvuDLGSit"
    }
  }
}
```

## 飞书 API 文档参考

- [飞书开放平台](https://open.feishu.cn/)
- [多维表格 API](https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/create)
- [获取 tenant_access_token](https://open.feishu.cn/document/server-docs/authentication-management/access-token/tenant_access_token_internal)

## 总结

`openclaw feishu-bitable` 不是一个 CLI 命令，而是 MCP 工具。对于批量数据操作场景，应该直接调用飞书开放平台 API，这样更简单、更高效、更可靠。
