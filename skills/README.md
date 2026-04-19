# OpenClaw Skill 框架使用指南

## 框架概述

所有接入 OpenClaw 系统的 Skill 必须遵循以下规范：

1. **继承 `BaseSkill` 基类**
2. **使用 `@register_skill` 装饰器**
3. **实现必需属性和方法**
4. **所有持久化数据库统一落在 `~/.openclaw/shared/data`，不要写进 skill 工作区**

## 数据库存储约定

从现在开始，所有会长期保存业务数据的 skill 都必须使用统一共享目录：

- 目录：`~/.openclaw/shared/data`
- 命名：`<skill_or_domain>.sqlite3` 或 `<skill_or_domain>.db`
- 禁止：把正式数据库直接放在 `skills/<skill>/data`、`workspace/skills` 根目录、`Downloads` 或临时输出目录

推荐直接复用 `base_skill.py` 中的工具函数：

```python
from skills.base_skill import get_shared_data_dir, get_shared_sqlite_path, get_shared_sqlite_url

db_url = get_shared_sqlite_url("my_skill")
db_path = get_shared_sqlite_path("my_skill")
```

这样做的目的：

- 避免同一个 skill 在 Desktop 工作区和 OpenClaw 工作区各存一份数据库
- 便于统一备份、迁移和排障
- 让后续新 skill 有固定存储落点，不再“各写各的”

## 快速开始

### 1. 创建新 Skill

```python
from skills.base_skill import BaseSkill, register_skill
from typing import Dict, Any

@register_skill
class MyNewSkill(BaseSkill):
    """
    我的自定义 Skill
    """
    
    @property
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        return "my_new_skill"
    
    @property
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        return "这是一个示例 Skill，用于演示框架使用方法"
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """参数的 JSON Schema（中文描述，便于主模型提取）"""
        return {
            "type": "object",
            "properties": {
                "param1": {
                    "type": "string",
                    "description": "参数1的描述，用中文详细说明参数的用途和格式"
                },
                "param2": {
                    "type": "integer",
                    "description": "参数2的描述，例如：数量，必须为正整数"
                },
                "param3": {
                    "type": "boolean",
                    "description": "参数3的描述，例如：是否启用某功能，默认为 False"
                }
            },
            "required": ["param1"]  # 必需参数列表
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行 Skill
        
        Args:
            **kwargs: 从主模型传来的参数
            
        Returns:
            执行结果字典，建议格式：
            {
                "success": True/False,
                "error": None 或错误信息,
                "data": 返回数据
            }
        """
        # 1. 参数验证
        validation_error = self.validate_params(**kwargs)
        if validation_error:
            return {"success": False, "error": validation_error, "data": None}
        
        # 2. 提取参数
        param1 = kwargs.get("param1")
        param2 = kwargs.get("param2", 0)  # 默认值
        param3 = kwargs.get("param3", False)
        
        # 3. 执行核心逻辑
        try:
            # 你的业务逻辑
            result = f"处理 {param1} 完成"
            
            return {
                "success": True,
                "error": None,
                "data": {"result": result}
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": None
            }
```

### 2. 包装旧代码（适配器模式）

如果已有旧代码，使用适配器模式包装：

```python
from skills.base_skill import BaseSkill, register_skill
from typing import Dict, Any

# 导入旧代码
from my_old_module import old_function

@register_skill
class LegacySkillAdapter(BaseSkill):
    """适配器：包装旧代码"""
    
    @property
    def name(self) -> str:
        return "legacy_skill"
    
    @property
    def description(self) -> str:
        return "包装旧功能的 Skill"
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_param": {
                    "type": "string",
                    "description": "输入参数描述"
                }
            },
            "required": ["input_param"]
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        input_param = kwargs.get("input_param")
        
        try:
            # 调用旧代码
            result = old_function(input_param)
            
            return {
                "success": True,
                "error": None,
                "data": result
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": None
            }
```

## JSON Schema 类型参考

```python
{
    "type": "object",
    "properties": {
        # 字符串类型
        "text_param": {
            "type": "string",
            "description": "字符串参数，例如：用户名、SKU、订单号等"
        },
        
        # 整数类型
        "integer_param": {
            "type": "integer",
            "description": "整数参数，例如：数量、页码、年龄等"
        },
        
        # 数字类型（整数或浮点数）
        "number_param": {
            "type": "number",
            "description": "数字参数，例如：价格、折扣率等"
        },
        
        # 布尔类型
        "boolean_param": {
            "type": "boolean",
            "description": "布尔参数，例如：是否启用模糊匹配、是否包含已删除等"
        },
        
        # 数组类型
        "array_param": {
            "type": "array",
            "items": {"type": "string"},
            "description": "数组参数，例如：SKU 列表、ID 列表等"
        },
        
        # 枚举类型
        "enum_param": {
            "type": "string",
            "enum": ["option1", "option2", "option3"],
            "description": "枚举参数，例如：国家代码（thailand/vietnam/philippines）"
        }
    },
    "required": ["text_param"]  # 必需参数
}
```

## 工具函数

```python
from skills.base_skill import (
    get_skill,           # 获取已注册的 Skill
    get_all_skills,      # 获取所有 Skill
    get_all_schemas,     # 获取所有 JSON Schema
    list_skills,         # 打印所有已注册的 Skill
    export_all_function_schemas,  # 导出用于 Kimi API 的 Schema
)

# 获取并执行 Skill
skill = get_skill("query_inventory")
result = skill.execute(sku_list=["TH-DR-8801"])

# 获取 Function Calling Schema（用于 Kimi API）
schemas = export_all_function_schemas()
# 返回格式：
# [
#   {
#     "type": "function",
#     "function": {
#       "name": "query_inventory",
#       "description": "...",
#       "parameters": {...}
#     }
#   }
# ]
```

## 文件结构

```
skills/
├── base_skill.py              # 基础框架（BaseSkill + register_skill）
├── inventory_skill_adapter.py # 库存查询 Skill（示例）
├── README.md                  # 本文档
└── inventory-query/           # 旧代码目录
    └── inventory_api.py       # 旧代码（未修改）
```

## 注意事项

1. **JSON Schema 描述必须用中文**：便于主模型（Kimi）准确理解参数含义
2. **参数验证**：调用 `self.validate_params(**kwargs)` 进行基础验证
3. **返回格式统一**：建议使用 `{"success": ..., "error": ..., "data": ...}` 格式
4. **异常处理**：在 `execute()` 中使用 try-except 捕获异常
5. **不修改旧代码**：使用适配器模式包装现有功能

## 已注册的 Skill

| Skill 名称 | 描述 | 文件 |
|-----------|------|------|
| query_inventory | 查询商品库存信息 | inventory_skill_adapter.py |
| generate_restock_report | 生成补货建议报告 | restock_skill_adapter.py |
| check_inventory_alert | 检查库存预警 | inventory_alert_adapter.py |
| process_creators | 处理达人 CRM 任务 | creator_crm_adapter.py |
| rerun_llm_analysis | 重新执行 LLM 分析（评分和打标） | rerun_llm_analysis.py |
