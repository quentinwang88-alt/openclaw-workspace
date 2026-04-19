#!/usr/bin/env python3
"""
Creator CRM Skill 适配器
使用适配器模式包装旧代码，使其符合新的 OpenClaw Skill 架构

设计原则：
- 不修改旧代码的核心逻辑
- 在 execute 方法中调用旧代码
- 将 Kimi 传来的参数正确映射并传递给旧代码
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

# 添加当前目录到路径，以便导入 base_skill
SKILLS_PATH = Path(__file__).parent
if str(SKILLS_PATH) not in sys.path:
    sys.path.insert(0, str(SKILLS_PATH))

# 添加 creator-crm 目录到路径，以便导入旧代码
CREATOR_CRM_PATH = Path(__file__).parent / "creator-crm"
if str(CREATOR_CRM_PATH) not in sys.path:
    sys.path.insert(0, str(CREATOR_CRM_PATH))

# 导入基础框架
from base_skill import BaseSkill, register_skill

# 导入旧代码
try:
    from run_pipeline import load_creators_from_feishu, load_creators_by_handles, run_pipeline
    CREATOR_CRM_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ 无法导入旧代码 run_pipeline: {e}")
    CREATOR_CRM_AVAILABLE = False


@register_skill
class CreatorCRMSkillAdapter(BaseSkill):
    """
    达人 CRM Skill 适配器
    
    包装旧的 run_pipeline.py 代码，使其符合新的 OpenClaw Skill 架构。
    主路由大脑（Kimi）可以通过 Function Calling 调用此 Skill。
    
    功能：
    1. 从飞书多维表格读取待处理达人
    2. 对每个达人执行完整流水线：
       - 从 Kalodata 获取视频数据
       - 生成 3x4 宫格图
       - 上传到飞书多维表格
       - 视频质量评分（LLM）
       - 带货标签打标（LLM）
    """
    
    @property
    def name(self) -> str:
        """Skill 名称（唯一标识符）"""
        return "process_creators"
    
    @property
    def description(self) -> str:
        """Skill 描述（用于主模型理解 Skill 功能）"""
        return (
            "处理达人 CRM 任务。"
            "从飞书多维表格读取待处理达人，执行完整流水线："
            "获取视频数据、生成宫格图、上传到飞书、LLM评分打标。"
            "支持指定处理数量、指定达人账号、测试模式等参数。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """
        参数的 JSON Schema
        
        参数说明：
        - limit: 处理数量限制
        - handles: 指定达人账号列表
        - dry_run: 测试模式
        - enable_llm: 是否启用 LLM 分析
        - max_workers: 并发数
        """
        return {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": (
                        "处理数量限制，最多处理的达人数量。"
                        "不指定则处理所有待开始的达人。"
                        "例如：设置为 5，则只处理前 5 个待开始的达人。"
                    )
                },
                "handles": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": (
                        "指定要处理的达人 TikTok 账号列表。"
                        "如果不指定，则处理所有状态为「待开始」的达人。"
                        "例如：['pimrypie', 'tingjasale']。"
                    )
                },
                "dry_run": {
                    "type": "boolean",
                    "description": (
                        "是否为测试模式（干运行）。"
                        "True：只显示将要处理的达人列表，不实际执行。"
                        "False（默认）：正常执行流水线。"
                    )
                },
                "enable_llm": {
                    "type": "boolean",
                    "description": (
                        "是否启用 LLM 分析（视频评分和带货标签）。"
                        "True（默认）：执行完整的 LLM 分析流程。"
                        "False：跳过 LLM 分析，只生成宫格图。"
                    )
                },
                "max_workers": {
                    "type": "integer",
                    "description": (
                        "并发处理数，同时处理的达人数量。"
                        "默认值为 2。"
                        "建议范围 1-3，过高可能导致 API 限流。"
                    )
                }
            },
            "required": []
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行达人 CRM 流水线
        
        Args:
            limit: 处理数量限制（可选）
            handles: 指定达人账号列表（可选）
            dry_run: 测试模式（可选，默认 False）
            enable_llm: 是否启用 LLM 分析（可选，默认 True）
            max_workers: 并发数（可选，默认 2）
            
        Returns:
            执行结果
        """
        # 检查旧代码是否可用
        if not CREATOR_CRM_AVAILABLE:
            return {
                "success": False,
                "error": "Creator CRM 模块未正确加载，请检查 run_pipeline.py 是否存在",
                "data": None
            }
        
        # 提取参数
        limit = kwargs.get("limit")
        handles = kwargs.get("handles")
        dry_run = kwargs.get("dry_run", False)
        enable_llm = kwargs.get("enable_llm", True)
        max_workers = kwargs.get("max_workers", 2)
        
        # 参数类型转换
        if isinstance(handles, str):
            handles = [handles]
        
        try:
            # 加载达人数据
            if handles:
                print(f"📌 指定处理达人: {handles}")
                creators = load_creators_by_handles(handles)
            else:
                creators = load_creators_from_feishu(limit=limit)
            
            if not creators:
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "message": "没有找到待处理的达人",
                        "stats": {
                            "total": 0,
                            "success": 0,
                            "failed": 0
                        }
                    }
                }
            
            # 执行流水线
            result = run_pipeline(
                creators=creators,
                max_workers=max_workers,
                dry_run=dry_run,
                enable_llm=enable_llm
            )
            
            # 构建结果
            stats = {
                "total": result.get('total', 0),
                "success": result.get('success', 0),
                "failed": result.get('failed', 0),
                "pending": result.get('pending', 0),
                "running": result.get('running', 0),
                "dry_run": result.get('dry_run', False)
            }
            
            # 生成消息
            if dry_run:
                message = f"测试模式：找到 {stats['total']} 个待处理达人"
            else:
                message = f"处理完成：共 {stats['total']} 个，成功 {stats['success']} 个，失败 {stats['failed']} 个"
            
            return {
                "success": True,
                "error": None,
                "data": {
                    "message": message,
                    "stats": stats,
                    "creators_count": len(creators)
                }
            }
            
        except Exception as e:
            import traceback
            return {
                "success": False,
                "error": f"流水线执行失败: {str(e)}",
                "data": None,
                "traceback": traceback.format_exc()
            }


# 便捷函数：直接调用
def process_creators(
    limit: Optional[int] = None,
    handles: Optional[List[str]] = None,
    dry_run: bool = False,
    enable_llm: bool = True,
    max_workers: int = 2
) -> Dict[str, Any]:
    """
    便捷函数：直接调用达人处理流水线
    
    Args:
        limit: 处理数量限制（可选）
        handles: 指定达人账号列表（可选）
        dry_run: 测试模式（默认 False）
        enable_llm: 是否启用 LLM 分析（默认 True）
        max_workers: 并发数（默认 2）
        
    Returns:
        执行结果
    """
    from base_skill import get_skill
    
    skill = get_skill("process_creators")
    if skill is None:
        raise RuntimeError("Creator CRM Skill 未注册")
    
    return skill.execute(
        limit=limit,
        handles=handles,
        dry_run=dry_run,
        enable_llm=enable_llm,
        max_workers=max_workers
    )


if __name__ == "__main__":
    # 测试适配器
    print("=" * 60)
    print("Creator CRM Skill 适配器测试")
    print("=" * 60)
    
    from base_skill import list_skills, get_skill
    import json
    
    # 显示已注册的 Skill
    list_skills()
    
    # 获取 Skill
    skill = get_skill("process_creators")
    if skill is None:
        print("❌ Skill 未注册")
        sys.exit(1)
    
    print(f"\n📦 Skill 名称: {skill.name}")
    print(f"📝 描述: {skill.description}")
    print(f"\n📋 JSON Schema:")
    print(json.dumps(skill.json_schema, indent=2, ensure_ascii=False))
    
    # 测试执行
    print("\n" + "=" * 60)
    print("测试执行（dry_run 模式）")
    print("=" * 60)
    
    # 测试：测试模式，只显示待处理达人
    print("\n测试: 测试模式检查待处理达人")
    result = skill.execute(dry_run=True, limit=5)
    
    if result.get("success"):
        print(f"\n✅ 测试成功")
        print(f"   消息: {result['data']['message']}")
        print(f"   统计: {result['data']['stats']}")
    else:
        print(f"\n❌ 测试失败: {result.get('error')}")