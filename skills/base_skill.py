#!/usr/bin/env python3
"""
OpenClaw Skill 基础框架
提供 BaseSkill 基类和 register_skill 装饰器
"""

import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, Callable, List


# 全局 Skill 注册表
_SKILL_REGISTRY: Dict[str, 'BaseSkill'] = {}

DEFAULT_SHARED_DATA_DIR = Path.home() / ".openclaw" / "shared" / "data"


def get_shared_data_dir() -> Path:
    """
    返回所有 Skill 持久化数据的统一目录。

    可通过 OPENCLAW_SHARED_DATA_DIR 覆盖，默认使用：
    ~/.openclaw/shared/data
    """
    raw = os.environ.get("OPENCLAW_SHARED_DATA_DIR", "").strip()
    path = Path(raw).expanduser() if raw else DEFAULT_SHARED_DATA_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_shared_sqlite_path(db_name: str) -> Path:
    """返回统一共享目录下的 SQLite 文件路径。"""
    filename = db_name if db_name.endswith((".db", ".sqlite", ".sqlite3")) else f"{db_name}.sqlite3"
    return get_shared_data_dir() / filename


def get_shared_sqlite_url(db_name: str) -> str:
    """返回统一共享目录下 SQLite 的 SQLAlchemy 风格 URL。"""
    return f"sqlite:///{get_shared_sqlite_path(db_name)}"


def register_skill(skill_class: type) -> type:
    """
    注册 Skill 装饰器
    
    使用方式:
        @register_skill
        class MySkill(BaseSkill):
            ...
    """
    if not issubclass(skill_class, BaseSkill):
        raise TypeError(f"{skill_class.__name__} 必须继承 BaseSkill")
    
    # 创建实例并注册
    instance = skill_class()
    skill_name = instance.name
    
    if skill_name in _SKILL_REGISTRY:
        raise ValueError(f"Skill '{skill_name}' 已注册，请使用不同的名称")
    
    _SKILL_REGISTRY[skill_name] = instance
    print(f"✅ 已注册 Skill: {skill_name}")
    
    return skill_class


def get_skill(name: str) -> Optional['BaseSkill']:
    """获取已注册的 Skill 实例"""
    return _SKILL_REGISTRY.get(name)


def get_all_skills() -> Dict[str, 'BaseSkill']:
    """获取所有已注册的 Skill"""
    return _SKILL_REGISTRY.copy()


def get_all_schemas() -> List[Dict[str, Any]]:
    """获取所有已注册 Skill 的 JSON Schema（用于 Function Calling）"""
    schemas = []
    for name, skill in _SKILL_REGISTRY.items():
        schemas.append({
            "name": skill.name,
            "description": skill.description,
            "parameters": skill.json_schema
        })
    return schemas


class BaseSkill(ABC):
    """
    Skill 基类
    
    所有接入 OpenClaw 系统的 Skill 必须继承此类，
    并实现以下属性和方法：
    - name: Skill 名称（唯一标识）
    - description: Skill 描述
    - json_schema: 参数的 JSON Schema
    - execute(**kwargs): 执行方法
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        pass
    
    @property
    @abstractmethod
    def json_schema(self) -> Dict[str, Any]:
        """
        参数的 JSON Schema
        
        必须符合 JSON Schema 规范，用于 Function Calling 参数验证。
        格式示例:
        {
            "type": "object",
            "properties": {
                "param1": {
                    "type": "string",
                    "description": "参数1的描述"
                }
            },
            "required": ["param1"]
        }
        """
        pass
    
    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行 Skill
        
        Args:
            **kwargs: 从主模型传来的参数
            
        Returns:
            执行结果字典
        """
        pass
    
    def validate_params(self, **kwargs) -> Optional[str]:
        """
        验证参数（可选实现）
        
        Returns:
            错误信息字符串，如果验证通过返回 None
        """
        required = self.json_schema.get("required", [])
        properties = self.json_schema.get("properties", {})
        
        for param in required:
            if param not in kwargs or kwargs[param] is None:
                return f"缺少必需参数: {param}"
        
        for param, value in kwargs.items():
            if param in properties and value is not None:
                expected_type = properties[param].get("type")
                if expected_type and not self._check_type(value, expected_type):
                    return f"参数 '{param}' 类型错误，期望 {expected_type}"
        
        return None
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        """检查值类型"""
        type_mapping = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict
        }
        
        # 特殊处理：integer 也接受 bool（Python 中 bool 是 int 的子类）
        if expected_type == "integer" and isinstance(value, bool):
            return False
        
        expected = type_mapping.get(expected_type)
        if expected is None:
            return True  # 未知类型，跳过检查
        
        return isinstance(value, expected)


def create_function_calling_schema(skill: BaseSkill) -> Dict[str, Any]:
    """
    创建用于 Function Calling 的完整 Schema
    
    适用于 Kimi、OpenAI 等 API 的 Function Calling 格式
    """
    return {
        "type": "function",
        "function": {
            "name": skill.name,
            "description": skill.description,
            "parameters": skill.json_schema
        }
    }


def export_all_function_schemas() -> List[Dict[str, Any]]:
    """
    导出所有已注册 Skill 的 Function Calling Schema
    
    可直接用于 Kimi API 的 tools 参数
    """
    return [create_function_calling_schema(skill) for skill in _SKILL_REGISTRY.values()]


# 便捷函数：打印所有已注册的 Skill
def list_skills():
    """打印所有已注册的 Skill"""
    if not _SKILL_REGISTRY:
        print("暂无已注册的 Skill")
        return
    
    print("=" * 60)
    print("已注册的 Skill 列表:")
    print("=" * 60)
    
    for name, skill in _SKILL_REGISTRY.items():
        print(f"\n📦 {name}")
        print(f"   描述: {skill.description}")
        required = skill.json_schema.get("required", [])
        print(f"   必需参数: {required}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    # 测试基础框架
    print("OpenClaw Skill 基础框架测试")
    print("=" * 60)
    
    # 示例：定义一个测试 Skill
    @register_skill
    class TestSkill(BaseSkill):
        @property
        def name(self) -> str:
            return "test_skill"
        
        @property
        def description(self) -> str:
            return "这是一个测试 Skill"
        
        @property
        def json_schema(self) -> Dict[str, Any]:
            return {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "测试消息"
                    }
                },
                "required": ["message"]
            }
        
        def execute(self, **kwargs) -> Dict[str, Any]:
            message = kwargs.get("message", "")
            return {"status": "success", "echo": message}
    
    # 测试
    list_skills()
    
    # 获取并执行
    skill = get_skill("test_skill")
    if skill:
        result = skill.execute(message="Hello, OpenClaw!")
        print(f"\n执行结果: {result}")
    
    # 导出 Function Calling Schema
    print("\nFunction Calling Schema:")
    print(json.dumps(export_all_function_schemas(), indent=2, ensure_ascii=False))
