#!/usr/bin/env python3
"""
库存查询 Skill 适配器
使用适配器模式包装旧代码，使其符合新的 OpenClaw Skill 架构

设计原则：
- 不修改旧代码的核心逻辑
- 在 execute 方法中调用旧代码
- 将 Kimi 传来的参数正确映射并传递给旧代码
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# 添加当前目录到路径，以便导入 base_skill
SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

# 添加 inventory-query 目录到路径，以便导入旧代码
INVENTORY_QUERY_PATH = Path(__file__).parent / "inventory-query"
if str(INVENTORY_QUERY_PATH) not in sys.path:
    sys.path.insert(0, str(INVENTORY_QUERY_PATH))

# 导入基础框架
from base_skill import BaseSkill, register_skill

# 导入旧代码
try:
    from inventory_api import query_inventory, InventoryAPI
except ImportError as e:
    print(f"⚠️ 无法导入旧代码 inventory_api: {e}")
    print(f"   请确保 skills/inventory-query/inventory_api.py 存在")
    query_inventory = None
    InventoryAPI = None


@register_skill
class LegacyInventorySkillAdapter(BaseSkill):
    """
    库存查询 Skill 适配器
    
    包装旧的 inventory_api.py 代码，使其符合新的 OpenClaw Skill 架构。
    主路由大脑（Kimi）可以通过 Function Calling 调用此 Skill。
    """
    
    @property
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        return "query_inventory"
    
    @property
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        return (
            "查询商品库存信息。"
            "支持单个或多个 SKU 查询，支持模糊匹配。"
            "返回库存数量、预留数量、总库存等详细信息。"
            "当模糊匹配到多个 SKU 时，会返回所有匹配项的汇总信息。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """
        参数的 JSON Schema
        
        参数说明：
        - sku_list: SKU 列表，必需参数，可以是单个 SKU 字符串或 SKU 列表
        - country: 国家代码，可选，默认使用配置中的默认国家
        - fuzzy: 是否模糊匹配，可选，默认 True
        """
        return {
            "type": "object",
            "properties": {
                "sku_list": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": (
                        "要查询的 SKU 列表。"
                        "SKU 是商品的唯一标识符，例如 'TH-DR-8801'、'bu0010'、'wj'。"
                        "可以传入单个 SKU 或多个 SKU。"
                        "当使用模糊匹配时，可以传入部分 SKU 名称，如 'wj' 会匹配所有包含 'wj' 的 SKU。"
                    )
                },
                "country": {
                    "type": "string",
                    "description": (
                        "国家代码，用于指定查询哪个国家的库存。"
                        "可选值：'thailand'（泰国）、'vietnam'（越南）、'philippines'（菲律宾）等。"
                        "如果不指定，将使用配置文件中的默认国家。"
                    )
                },
                "fuzzy": {
                    "type": "boolean",
                    "description": (
                        "是否使用模糊匹配模式。"
                        "True（默认）：模糊匹配，查询 'wj' 会返回所有包含 'wj' 的 SKU，如 'WJ-001'、'WJ-002'。"
                        "False：精确匹配，只返回完全匹配的 SKU。"
                        "模糊匹配适用于不确定完整 SKU 名称的情况，精确匹配适用于已知确切 SKU 的情况。"
                    )
                },
                "profile": {
                    "type": "string",
                    "description": (
                        "可选的 BigSeller 账号 profile 名称。"
                        "适用于 inventory-query 已保存多个店铺账号配置的场景。"
                        "不传时使用当前激活账号。"
                    )
                }
            },
            "required": ["sku_list"]
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行库存查询
        
        Args:
            sku_list: SKU 列表（必需）
            country: 国家代码（可选）
            fuzzy: 是否模糊匹配（可选，默认 True）
            
        Returns:
            查询结果字典，包含每个 SKU 的库存信息
        """
        # 检查旧代码是否可用
        if query_inventory is None:
            return {
                "success": False,
                "error": "库存查询模块未正确加载，请检查 inventory_api.py 是否存在",
                "data": None
            }
        
        # 提取参数（先提取，以便进行类型转换）
        sku_list = kwargs.get("sku_list", [])
        country = kwargs.get("country")
        fuzzy = kwargs.get("fuzzy", True)  # 默认使用模糊匹配
        profile = kwargs.get("profile")
        
        # 参数类型转换：支持传入单个字符串
        if isinstance(sku_list, str):
            sku_list = [sku_list]
        
        # 更新 kwargs 以便验证
        kwargs = {"sku_list": sku_list, "country": country, "fuzzy": fuzzy, "profile": profile}
        
        # 参数验证
        validation_error = self.validate_params(**kwargs)
        if validation_error:
            return {
                "success": False,
                "error": validation_error,
                "data": None
            }
        
        # 确保 sku_list 是列表
        if not isinstance(sku_list, list):
            return {
                "success": False,
                "error": f"sku_list 参数类型错误，期望 list 或 str，收到 {type(sku_list).__name__}",
                "data": None
            }
        
        # 空列表检查
        if not sku_list:
            return {
                "success": False,
                "error": "sku_list 不能为空",
                "data": None
            }
        
        try:
            # 调用旧代码
            results = query_inventory(
                sku_list=sku_list,
                country=country,
                fuzzy=fuzzy,
                profile=profile
            )
            
            # 格式化返回结果
            formatted_results = self._format_results(results)
            
            return {
                "success": True,
                "error": None,
                "data": formatted_results,
                "summary": self._generate_summary(formatted_results)
            }
            
        except FileNotFoundError as e:
            return {
                "success": False,
                "error": f"配置文件未找到: {str(e)}",
                "data": None,
                "suggestion": "请确保 skills/inventory-query/config/api_config.json 存在并正确配置"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"查询失败: {str(e)}",
                "data": None
            }
    
    def _format_results(self, results: Dict[str, dict]) -> Dict[str, Any]:
        """
        格式化查询结果，使其更易于理解
        
        Args:
            results: 原始查询结果
            
        Returns:
            格式化后的结果
        """
        formatted = {}
        
        for sku, data in results.items():
            if "error" in data:
                # 错误情况
                formatted[sku] = {
                    "status": "error",
                    "error": data["error"],
                    "error_type": data.get("error_type", "unknown"),
                    "suggestion": data.get("suggestion")
                }
            elif "matched_count" in data:
                # 多个匹配（模糊匹配结果）
                formatted[sku] = {
                    "status": "multiple_matches",
                    "matched_count": data["matched_count"],
                    "matched_skus": data["matched_skus"],
                    "total_available": data["available"],
                    "total_onhand": data["total"],
                    "total_reserved": data["reserved"]
                }
            else:
                # 单个匹配
                formatted[sku] = {
                    "status": "success",
                    "sku": data.get("sku", sku),
                    "available": data["available"],
                    "total": data["total"],
                    "reserved": data["reserved"],
                    "inventory_status": data.get("status", "unknown")
                }
        
        return formatted
    
    def _generate_summary(self, formatted_results: Dict[str, Any]) -> str:
        """
        生成查询结果摘要
        
        Args:
            formatted_results: 格式化后的结果
            
        Returns:
            摘要字符串
        """
        total = len(formatted_results)
        success = sum(1 for v in formatted_results.values() if v["status"] == "success")
        multiple = sum(1 for v in formatted_results.values() if v["status"] == "multiple_matches")
        errors = sum(1 for v in formatted_results.values() if v["status"] == "error")
        
        summary_parts = [f"查询了 {total} 个 SKU"]
        
        if success > 0:
            summary_parts.append(f"{success} 个精确匹配")
        if multiple > 0:
            summary_parts.append(f"{multiple} 个模糊匹配（多个结果）")
        if errors > 0:
            summary_parts.append(f"{errors} 个查询失败")
        
        return "，".join(summary_parts)


# 便捷函数：直接调用
def query(
    sku_list: List[str],
    country: Optional[str] = None,
    fuzzy: bool = True,
    profile: Optional[str] = None
) -> Dict[str, Any]:
    """
    便捷函数：直接调用库存查询
    
    Args:
        sku_list: SKU 列表
        country: 国家代码（可选）
        fuzzy: 是否模糊匹配（默认 True）
        profile: 指定账号 profile
        
    Returns:
        查询结果
    """
    from base_skill import get_skill
    
    skill = get_skill("query_inventory")
    if skill is None:
        raise RuntimeError("库存查询 Skill 未注册")
    
    return skill.execute(sku_list=sku_list, country=country, fuzzy=fuzzy, profile=profile)


if __name__ == "__main__":
    # 测试适配器
    print("=" * 60)
    print("库存查询 Skill 适配器测试")
    print("=" * 60)
    
    from base_skill import list_skills, get_skill
    
    # 显示已注册的 Skill
    list_skills()
    
    # 获取 Skill
    skill = get_skill("query_inventory")
    if skill is None:
        print("❌ Skill 未注册")
        sys.exit(1)
    
    print(f"\n📦 Skill 名称: {skill.name}")
    print(f"📝 描述: {skill.description}")
    print(f"\n📋 JSON Schema:")
    import json
    print(json.dumps(skill.json_schema, indent=2, ensure_ascii=False))
    
    # 测试执行
    print("\n" + "=" * 60)
    print("测试执行")
    print("=" * 60)
    
    # 测试 1：查询不存在的 SKU（应该返回错误）
    print("\n测试 1: 查询不存在的 SKU")
    result = skill.execute(sku_list=["NOT_EXIST_SKU_12345"])
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 测试 2：参数验证
    print("\n测试 2: 参数验证（缺少必需参数）")
    result = skill.execute()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 测试 3：参数类型转换
    print("\n测试 3: 参数类型转换（单个字符串 SKU）")
    result = skill.execute(sku_list="bu0010")
    print(json.dumps(result, indent=2, ensure_ascii=False))
