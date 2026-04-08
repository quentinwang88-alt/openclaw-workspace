#!/usr/bin/env python3
"""
飞书集成脚本 - 批量分析达人并更新多维表格

功能：
1. 从飞书多维表格读取达人列表（包含 tk_profile_url）
2. 使用 Playwright 极简方案批量分析达人风格
3. 将分析结果写回飞书多维表格
"""

import sys
import os
import json
import time
from typing import List, Dict, Any, Optional

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.data_schema import CreatorCRM, ApparelStyle, AccessoryStyle
from core.analyze_creator_vibe_v2 import analyze_creator_vibe_v2


class FeishuCreatorAnalyzer:
    """飞书达人分析器"""
    
    def __init__(
        self,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        table_id: Optional[str] = None
    ):
        """
        初始化飞书分析器
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
            table_id: 多维表格 ID
        """
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        self.table_id = table_id or os.getenv("FEISHU_TABLE_ID")
        
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，将使用模拟模式")
    
    def fetch_creators_from_feishu(self) -> List[Dict[str, Any]]:
        """
        从飞书多维表格获取达人列表
        
        Returns:
            达人列表
        """
        print("📡 从飞书多维表格获取达人列表...")
        
        # TODO: 实际的飞书 API 调用
        # 这里需要使用飞书 SDK 或 HTTP API
        # 示例代码：
        # import lark_oapi as lark
        # client = lark.Client.builder().app_id(self.app_id).app_secret(self.app_secret).build()
        # response = client.bitable.v1.app_table_record.list(...)
        
        # 模拟数据（用于测试）
        mock_creators = [
            {
                "record_id": "rec001",
                "creator_id": "CR-2026-001",
                "tk_handle": "@fashionista_th",
                "tk_profile_url": "https://www.tiktok.com/@fashionista_th",
                "followers_count": 50000,
                "ai_apparel_style": None,
                "ai_accessory_style": None
            },
            {
                "record_id": "rec002",
                "creator_id": "CR-2026-002",
                "tk_handle": "@minimal_style",
                "tk_profile_url": "https://www.tiktok.com/@minimal_style",
                "followers_count": 30000,
                "ai_apparel_style": None,
                "ai_accessory_style": None
            },
            {
                "record_id": "rec003",
                "creator_id": "CR-2026-003",
                "tk_handle": "@sweet_girl",
                "tk_profile_url": "https://www.tiktok.com/@sweet_girl",
                "followers_count": 20000,
                "ai_apparel_style": None,
                "ai_accessory_style": None
            }
        ]
        
        print(f"✅ 获取到 {len(mock_creators)} 个达人")
        return mock_creators
    
    def analyze_creator(
        self,
        creator: Dict[str, Any],
        use_mock: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        分析单个达人
        
        Args:
            creator: 达人信息
            use_mock: 是否使用模拟模式
            
        Returns:
            分析结果
        """
        tk_profile_url = creator.get("tk_profile_url")
        
        if not tk_profile_url:
            print(f"⚠️ 达人 {creator.get('tk_handle')} 缺少主页 URL，跳过")
            return None
        
        try:
            result = analyze_creator_vibe_v2(
                tk_profile_url=tk_profile_url,
                use_mock=use_mock
            )
            
            return {
                "record_id": creator.get("record_id"),
                "ai_apparel_style": result.ai_apparel_style,
                "ai_accessory_style": result.ai_accessory_style,
                "preferred_category": result.preferred_category,
                "analysis_reason": result.analysis_reason
            }
            
        except Exception as e:
            print(f"❌ 分析失败: {str(e)}")
            return None
    
    def update_feishu_record(
        self,
        record_id: str,
        analysis_result: Dict[str, Any]
    ) -> bool:
        """
        更新飞书多维表格记录
        
        Args:
            record_id: 记录 ID
            analysis_result: 分析结果
            
        Returns:
            是否成功
        """
        print(f"📝 更新飞书记录: {record_id}")
        
        # TODO: 实际的飞书 API 调用
        # 示例代码：
        # client.bitable.v1.app_table_record.update(
        #     request=lark.UpdateAppTableRecordRequest.builder()
        #         .app_token(self.table_id)
        #         .table_id(table_id)
        #         .record_id(record_id)
        #         .fields({
        #             "ai_apparel_style": analysis_result["ai_apparel_style"],
        #             "ai_accessory_style": analysis_result["ai_accessory_style"]
        #         })
        #         .build()
        # )
        
        # 模拟成功
        print(f"   ✅ 已更新: {analysis_result['ai_apparel_style']}")
        return True
    
    def batch_analyze(
        self,
        use_mock: bool = False,
        delay_seconds: int = 2
    ) -> Dict[str, Any]:
        """
        批量分析达人
        
        Args:
            use_mock: 是否使用模拟模式
            delay_seconds: 每次分析之间的延迟（秒）
            
        Returns:
            分析统计
        """
        print("=" * 70)
        print("🚀 开始批量分析达人")
        print("=" * 70)
        print()
        
        # 1. 获取达人列表
        creators = self.fetch_creators_from_feishu()
        
        # 过滤出未分析的达人
        pending_creators = [
            c for c in creators
            if not c.get("ai_apparel_style")
        ]
        
        print(f"📊 待分析达人: {len(pending_creators)}/{len(creators)}")
        print()
        
        # 2. 批量分析
        success_count = 0
        failed_count = 0
        
        for i, creator in enumerate(pending_creators, 1):
            print(f"[{i}/{len(pending_creators)}] 分析: {creator['tk_handle']}")
            
            # 分析达人
            result = self.analyze_creator(creator, use_mock=use_mock)
            
            if result:
                # 更新飞书记录
                if self.update_feishu_record(result["record_id"], result):
                    success_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1
            
            print()
            
            # 延迟（避免限流）
            if i < len(pending_creators):
                time.sleep(delay_seconds)
        
        # 3. 统计结果
        stats = {
            "total": len(creators),
            "pending": len(pending_creators),
            "success": success_count,
            "failed": failed_count
        }
        
        print("=" * 70)
        print("📊 批量分析完成")
        print("=" * 70)
        print(f"总达人数: {stats['total']}")
        print(f"待分析数: {stats['pending']}")
        print(f"成功数: {stats['success']}")
        print(f"失败数: {stats['failed']}")
        print()
        
        return stats


def main():
    """主函数"""
    print()
    print("🚀 飞书达人批量分析工具")
    print()
    
    # 创建分析器
    analyzer = FeishuCreatorAnalyzer()
    
    # 批量分析（使用模拟模式）
    stats = analyzer.batch_analyze(
        use_mock=True,  # 设置为 False 使用真实 API
        delay_seconds=2
    )
    
    # 保存统计结果
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, 'batch_analysis_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    print(f"📝 统计结果已保存到: {output_path}")
    print()
    
    print("💡 下一步:")
    print("  1. 配置飞书 API: export FEISHU_APP_ID='xxx' FEISHU_APP_SECRET='xxx'")
    print("  2. 设置表格 ID: export FEISHU_TABLE_ID='xxx'")
    print("  3. 修改 use_mock=False 使用真实 API")
    print("  4. 运行: python3 utils/feishu_batch_analyzer.py")
    print()


if __name__ == "__main__":
    main()
