#!/usr/bin/env python3
"""
重新执行 LLM 分析 Skill
用于重新执行视频评分和带货标签打标，不需要重新抓取视频封面

使用场景：
- LLM 分析失败需要重试
- 需要更新评分或标签
- 批量补充 LLM 分析

使用方法：
    # 重新执行所有缺失 LLM 分析的记录
    python3 skills/rerun_llm_analysis.py
    
    # 重新执行指定达人
    python3 skills/rerun_llm_analysis.py --handles pimrypie tingjasale
    
    # 只执行视频评分
    python3 skills/rerun_llm_analysis.py --scoring-only
    
    # 只执行带货标签打标
    python3 skills/rerun_llm_analysis.py --tagging-only
    
    # 测试模式（不更新飞书）
    python3 skills/rerun_llm_analysis.py --dry-run
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# 添加路径
SKILL_DIR = Path(__file__).parent
sys.path.insert(0, str(SKILL_DIR))
sys.path.insert(0, str(SKILL_DIR / "creator-crm"))

# 导入基础框架
from base_skill import BaseSkill, register_skill

# 导入 LLM 分析模块
try:
    from core.llm_analyzer import (
        VideoScoringAgent, CategoryTaggingAgent, FeishuFieldUpdater
    )
    from core.feishu_reader import FeishuBitableReader
    LLM_ANALYZER_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ 无法导入 LLM 分析模块: {e}")
    LLM_ANALYZER_AVAILABLE = False


# 飞书配置
FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")


def find_grid_image(tk_handle: str) -> Optional[str]:
    """
    查找达人的宫格图
    
    Args:
        tk_handle: TikTok 账号名
        
    Returns:
        宫格图路径，如果找不到返回 None
    """
    # 可能的宫格图目录
    grid_dirs = [
        SKILL_DIR / "creator-crm" / "output" / "grids",
        SKILL_DIR / "output" / "grids",
    ]
    
    # 可能的文件名格式
    patterns = [
        f"{tk_handle}_grid.png",
        f"{tk_handle}.en_grid.png",
        f"{tk_handle}_video_grid.png",
    ]
    
    for grid_dir in grid_dirs:
        if not grid_dir.exists():
            continue
        
        for pattern in patterns:
            grid_path = grid_dir / pattern
            if grid_path.exists():
                return str(grid_path)
    
    return None


def load_creators_with_grids(
    handles: Optional[List[str]] = None,
    filter_missing_llm: bool = True,
    force_all: bool = False
) -> List[Dict[str, Any]]:
    """
    从飞书加载有宫格图的达人
    
    Args:
        handles: 指定达人账号列表（可选）
        filter_missing_llm: 是否只返回缺失 LLM 分析的记录
        force_all: 是否强制处理所有有宫格图的达人（忽略已有的评分/标签）
        
    Returns:
        达人列表
    """
    print(f"\n📖 从飞书读取达人数据...")
    
    try:
        reader = FeishuBitableReader(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_TABLE_ID
        )
        
        records = reader.read_records(page_size=500)
        print(f"✅ 读取到 {len(records)} 条记录")
        
        creators = []
        skipped_count = 0
        
        for record in records:
            # 过滤条件：有宫格图且状态为"已完成"
            if not record.tk_handle:
                continue
            
            # 如果指定了账号，只处理指定账号
            if handles and record.tk_handle not in handles:
                continue
            
            # 查找宫格图
            grid_path = find_grid_image(record.tk_handle)
            if not grid_path:
                continue
            
            # 检查是否缺失 LLM 分析
            score_field = getattr(record, '视频质量评分', None) or getattr(record, 'video_score', None)
            tag_field = getattr(record, '带货标签', None) or getattr(record, 'category_tags', None)
            
            # 如果不是强制全部重新处理，且两个字段都有值，跳过
            if not force_all and filter_missing_llm:
                if score_field and tag_field:
                    skipped_count += 1
                    continue
            
            creators.append({
                'record_id': record.record_id,
                'tk_handle': record.tk_handle,
                'grid_path': grid_path,
                'has_score': bool(score_field),
                'has_tag': bool(tag_field)
            })
        
        if skipped_count > 0:
            print(f"   跳过 {skipped_count} 个已有评分和标签的达人")
        print(f"✅ 找到 {len(creators)} 个需要 LLM 分析的达人")
        return creators
        
    except Exception as e:
        print(f"⚠️ 从飞书读取失败: {e}")
        return []


@register_skill
class RerunLLMAnalysisSkillAdapter(BaseSkill):
    """
    重新执行 LLM 分析 Skill
    
    用于重新执行视频评分和带货标签打标，不需要重新抓取视频封面。
    主路由大脑（Kimi）可以通过 Function Calling 调用此 Skill。
    """
    
    @property
    def name(self) -> str:
        """Skill 名称"""
        return "rerun_llm_analysis"
    
    @property
    def description(self) -> str:
        """Skill 描述"""
        return (
            "重新执行达人视频的 LLM 分析（评分和打标）。"
            "不需要重新抓取视频封面，直接使用已生成的宫格图。"
            "支持只执行评分、只执行打标、或两者都执行。"
            "适用于 LLM 分析失败需要重试的场景。"
        )
    
    @property
    def json_schema(self) -> Dict[str, Any]:
        """参数 JSON Schema"""
        return {
            "type": "object",
            "properties": {
                "handles": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "指定要重新分析的达人 TikTok 账号列表。"
                        "如果不指定，则处理所有符合条件的达人。"
                        "例如：['pimrypie', 'tingjasale']。"
                    )
                },
                "analysis_type": {
                    "type": "string",
                    "enum": ["both", "scoring", "tagging"],
                    "description": (
                        "分析类型。"
                        "both（默认）：同时执行视频评分和带货标签打标。"
                        "scoring：只执行视频质量评分。"
                        "tagging：只执行带货标签打标。"
                    )
                },
                "force_all": {
                    "type": "boolean",
                    "description": (
                        "是否强制对所有有宫格图的达人重新执行分析。"
                        "True：重新处理所有有宫格图的达人，覆盖已有的评分和标签。"
                        "False（默认）：只处理缺失评分或标签的达人。"
                    )
                },
                "dry_run": {
                    "type": "boolean",
                    "description": (
                        "是否为测试模式。"
                        "True：只显示将要处理的达人列表，不实际执行。"
                        "False（默认）：正常执行并更新飞书表格。"
                    )
                },
                "limit": {
                    "type": "integer",
                    "description": (
                        "处理数量限制，最多处理的达人数量。"
                        "不指定则处理所有符合条件的达人。"
                    )
                }
            },
            "required": []
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行 LLM 分析
        
        Args:
            handles: 指定达人账号列表（可选）
            analysis_type: 分析类型（可选，默认 both）
            force_all: 强制处理所有有宫格图的达人（可选，默认 False）
            dry_run: 测试模式（可选，默认 False）
            limit: 处理数量限制（可选）
            
        Returns:
            执行结果
        """
        if not LLM_ANALYZER_AVAILABLE:
            return {
                "success": False,
                "error": "LLM 分析模块未正确加载",
                "data": None
            }
        
        # 提取参数
        handles = kwargs.get("handles")
        analysis_type = kwargs.get("analysis_type", "both")
        force_all = kwargs.get("force_all", False)
        dry_run = kwargs.get("dry_run", False)
        limit = kwargs.get("limit")
        
        # 类型转换
        if isinstance(handles, str):
            handles = [handles]
        
        try:
            # 加载达人数据
            creators = load_creators_with_grids(
                handles=handles,
                filter_missing_llm=not force_all,  # force_all=True 时不过滤
                force_all=force_all
            )
            
            if not creators:
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "message": "没有找到需要 LLM 分析的达人",
                        "stats": {"total": 0, "success": 0, "failed": 0}
                    }
                }
            
            # 限制数量
            if limit:
                creators = creators[:limit]
            
            # 测试模式
            if dry_run:
                return {
                    "success": True,
                    "error": None,
                    "data": {
                        "message": f"测试模式：找到 {len(creators)} 个需要 LLM 分析的达人",
                        "creators": [
                            {
                                "tk_handle": c['tk_handle'],
                                "grid_path": c['grid_path']
                            }
                            for c in creators
                        ],
                        "stats": {"total": len(creators), "success": 0, "failed": 0, "dry_run": True}
                    }
                }
            
            # 初始化 Agent
            scoring_agent = None
            tagging_agent = None
            feishu_updater = FeishuFieldUpdater()
            
            if analysis_type in ["both", "scoring"]:
                scoring_agent = VideoScoringAgent()
            
            if analysis_type in ["both", "tagging"]:
                tagging_agent = CategoryTaggingAgent()
            
            # 执行分析
            stats = {"total": len(creators), "success": 0, "failed": 0, "results": []}
            
            for i, creator in enumerate(creators, 1):
                tk_handle = creator['tk_handle']
                grid_path = creator['grid_path']
                record_id = creator['record_id']
                
                print(f"\n[{i}/{len(creators)}] 处理达人: {tk_handle}")
                print(f"  宫格图: {Path(grid_path).name}")
                
                result = {
                    "tk_handle": tk_handle,
                    "record_id": record_id,
                    "scoring_success": False,
                    "tagging_success": False
                }
                
                try:
                    # 执行视频评分
                    if scoring_agent:
                        print(f"  🎯 执行视频评分...")
                        scoring_result = scoring_agent.execute({
                            'tk_handle': tk_handle,
                            'grid_paths': [grid_path]
                        })
                        
                        if scoring_result:
                            result['scoring_success'] = True
                            result['scoring_result'] = scoring_result
                            
                            # 更新飞书
                            feishu_updater.update_fields(
                                record_id=record_id,
                                fields={
                                    "视频质量评分": json.dumps(scoring_result, ensure_ascii=False),
                                    "视频最终评分": str(scoring_result.get('final_star_rating', ''))
                                }
                            )
                            print(f"     ✅ 评分完成: {scoring_result.get('final_star_rating')}/5.0")
                    
                    # 执行带货标签打标
                    if tagging_agent:
                        print(f"  🏷️ 执行带货标签打标...")
                        tagging_result = tagging_agent.execute({
                            'tk_handle': tk_handle,
                            'grid_paths': [grid_path]
                        })
                        
                        if tagging_result:
                            result['tagging_success'] = True
                            result['tagging_result'] = tagging_result
                            
                            # 更新飞书
                            tags_str = ", ".join(tagging_result.get('subcategory_tags', []))
                            feishu_updater.update_fields(
                                record_id=record_id,
                                fields={
                                    "带货标签": tags_str,
                                    "带货大类": tagging_result.get('primary_category', '')
                                }
                            )
                            print(f"     ✅ 打标完成: {tags_str}")
                    
                    # 统计
                    if result['scoring_success'] or result['tagging_success']:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                    
                    stats['results'].append(result)
                    
                except Exception as e:
                    print(f"     ❌ 处理失败: {e}")
                    stats['failed'] += 1
                    result['error'] = str(e)
                    stats['results'].append(result)
            
            # 生成消息
            message = (
                f"LLM 分析完成：共 {stats['total']} 个，"
                f"成功 {stats['success']} 个，失败 {stats['failed']} 个"
            )
            
            return {
                "success": True,
                "error": None,
                "data": {
                    "message": message,
                    "stats": {
                        "total": stats['total'],
                        "success": stats['success'],
                        "failed": stats['failed']
                    },
                    "results": stats['results']
                }
            }
            
        except Exception as e:
            import traceback
            return {
                "success": False,
                "error": f"LLM 分析失败: {str(e)}",
                "data": None,
                "traceback": traceback.format_exc()
            }


# 便捷函数
def rerun_llm_analysis(
    handles: Optional[List[str]] = None,
    analysis_type: str = "both",
    force_all: bool = False,
    dry_run: bool = False,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    便捷函数：重新执行 LLM 分析
    
    Args:
        handles: 指定达人账号列表（可选）
        analysis_type: 分析类型（默认 both）
        force_all: 强制处理所有有宫格图的达人（默认 False）
        dry_run: 测试模式（默认 False）
        limit: 处理数量限制（可选）
        
    Returns:
        执行结果
    """
    from base_skill import get_skill
    
    skill = get_skill("rerun_llm_analysis")
    if skill is None:
        raise RuntimeError("LLM 分析 Skill 未注册")
    
    return skill.execute(
        handles=handles,
        analysis_type=analysis_type,
        force_all=force_all,
        dry_run=dry_run,
        limit=limit
    )


if __name__ == "__main__":
    # 测试适配器
    print("=" * 60)
    print("重新执行 LLM 分析 Skill 测试")
    print("=" * 60)
    
    from base_skill import list_skills, get_skill
    import json
    
    # 显示已注册的 Skill
    list_skills()
    
    # 获取 Skill
    skill = get_skill("rerun_llm_analysis")
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
    
    result = skill.execute(dry_run=True, limit=5)
    
    if result.get("success"):
        print(f"\n✅ 测试成功")
        print(f"   消息: {result['data']['message']}")
        if result['data'].get('creators'):
            print(f"   找到的达人:")
            for c in result['data']['creators'][:5]:
                print(f"     - {c['tk_handle']}: {Path(c['grid_path']).name}")
    else:
        print(f"\n❌ 测试失败: {result.get('error')}")