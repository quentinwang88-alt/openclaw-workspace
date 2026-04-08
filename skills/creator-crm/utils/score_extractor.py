#!/usr/bin/env python3
"""
飞书评分提取器 - 从大模型分析结果中提取最终得分

功能：
1. 读取飞书多维表格中的大模型分析结果
2. 清洗和解析 JSON 格式的分析结果
3. 提取最终得分并写回飞书表格
"""

import sys
import os
import json
import re
from typing import Optional, Dict, Any, List

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ScoreExtractor:
    """评分提取器"""
    
    def __init__(
        self,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        table_id: Optional[str] = None
    ):
        """
        初始化评分提取器
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
            table_id: 多维表格 ID
        """
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        self.table_id = table_id or os.getenv("FEISHU_TABLE_ID")
    
    def extract_final_score(self, llm_response_text: str) -> Optional[float]:
        """
        清洗大模型输出的文本，解析 JSON，并直接提取最终分数
        
        Args:
            llm_response_text: 大模型输出的原始文本
            
        Returns:
            Optional[float]: 最终得分，失败返回 None
            
        Example:
            >>> text = '''```json
            ... {
            ...   "final_star_rating": 4.5,
            ...   "details": {...}
            ... }
            ... ```'''
            >>> extractor.extract_final_score(text)
            4.5
        """
        try:
            # 第一步：强力清洗
            # 用正则去掉大模型可能带上的 ```json 和 ``` 标记
            cleaned_text = re.sub(
                r'```(?:json)?\s*|\s*```',  # 匹配 ```json 或 ``` 及其周围空白
                '',
                llm_response_text
            ).strip()
            
            # 第二步：解析 JSON
            result = json.loads(cleaned_text)
            
            # 第三步：提取最终得分
            # 支持多种可能的字段名
            score_fields = [
                "final_star_rating",
                "final_score",
                "total_score",
                "overall_rating",
                "rating"
            ]
            
            for field in score_fields:
                if field in result:
                    score = result[field]
                    # 确保返回浮点数
                    return float(score)
            
            # 如果没有找到任何得分字段
            print(f"⚠️ 未找到得分字段，可用字段: {list(result.keys())}")
            return None
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON 解析失败: {str(e)}")
            print(f"   清洗后的文本: {cleaned_text[:200]}...")
            return None
        except Exception as e:
            print(f"❌ 提取失败: {str(e)}")
            return None
    
    def read_record_from_feishu(
        self,
        record_id: str,
        field_name: str
    ) -> Optional[str]:
        """
        从飞书多维表格读取记录的指定字段
        
        Args:
            record_id: 记录 ID
            field_name: 字段名称
            
        Returns:
            Optional[str]: 字段内容
        """
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，使用模拟模式")
            # 模拟返回
            return '''```json
{
  "final_star_rating": 4.5,
  "content_quality": 4.0,
  "visual_appeal": 5.0,
  "engagement": 4.5,
  "analysis": "视频质量优秀，画面清晰，内容吸引人..."
}
```'''
        
        try:
            # TODO: 实际的飞书 API 调用
            # import requests
            # 
            # access_token = self.get_access_token()
            # url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.table_id}/tables/{table_id}/records/{record_id}"
            # 
            # headers = {
            #     "Authorization": f"Bearer {access_token}"
            # }
            # 
            # response = requests.get(url, headers=headers)
            # result = response.json()
            # 
            # if result.get("code") == 0:
            #     fields = result.get("data", {}).get("record", {}).get("fields", {})
            #     return fields.get(field_name)
            # else:
            #     raise Exception(f"读取失败: {result.get('msg')}")
            
            # 模拟返回
            return '''```json
{
  "final_star_rating": 4.5,
  "content_quality": 4.0,
  "visual_appeal": 5.0,
  "engagement": 4.5
}
```'''
            
        except Exception as e:
            print(f"❌ 读取记录失败: {str(e)}")
            return None
    
    def write_score_to_feishu(
        self,
        record_id: str,
        field_name: str,
        score: float
    ) -> bool:
        """
        将提取的得分写回飞书多维表格
        
        Args:
            record_id: 记录 ID
            field_name: 字段名称
            score: 得分
            
        Returns:
            bool: 是否成功
        """
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，使用模拟模式")
            print(f"📝 模拟写入: 记录 {record_id} 的字段 {field_name} = {score}")
            return True
        
        try:
            # TODO: 实际的飞书 API 调用
            # import requests
            # 
            # access_token = self.get_access_token()
            # url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.table_id}/tables/{table_id}/records/{record_id}"
            # 
            # headers = {
            #     "Authorization": f"Bearer {access_token}",
            #     "Content-Type": "application/json"
            # }
            # 
            # payload = {
            #     "fields": {
            #         field_name: score
            #     }
            # }
            # 
            # response = requests.put(url, headers=headers, json=payload)
            # result = response.json()
            # 
            # if result.get("code") == 0:
            #     return True
            # else:
            #     raise Exception(f"写入失败: {result.get('msg')}")
            
            # 模拟写入
            print(f"📝 模拟写入: 记录 {record_id} 的字段 {field_name} = {score}")
            return True
            
        except Exception as e:
            print(f"❌ 写入得分失败: {str(e)}")
            return False
    
    def process_record(
        self,
        record_id: str,
        source_field: str,
        target_field: str
    ) -> Optional[float]:
        """
        处理单条记录：读取 → 提取 → 写入
        
        Args:
            record_id: 记录 ID
            source_field: 源字段名（大模型分析结果）
            target_field: 目标字段名（最终得分）
            
        Returns:
            Optional[float]: 提取的得分
        """
        print(f"🔄 处理记录: {record_id}")
        
        # 1. 读取大模型分析结果
        llm_response = self.read_record_from_feishu(record_id, source_field)
        
        if not llm_response:
            print(f"❌ 记录 {record_id}: 未找到分析结果")
            return None
        
        # 2. 提取最终得分
        score = self.extract_final_score(llm_response)
        
        if score is None:
            print(f"❌ 记录 {record_id}: 提取得分失败")
            return None
        
        print(f"✅ 记录 {record_id}: 提取得分 = {score}")
        
        # 3. 写回飞书表格
        success = self.write_score_to_feishu(record_id, target_field, score)
        
        if success:
            print(f"✅ 记录 {record_id}: 写入成功")
        else:
            print(f"❌ 记录 {record_id}: 写入失败")
        
        return score
    
    def process_all_records(
        self,
        record_ids: List[str],
        source_field: str = "视频质量评分",
        target_field: str = "最终得分"
    ) -> Dict[str, Any]:
        """
        批量处理所有记录
        
        Args:
            record_ids: 记录 ID 列表
            source_field: 源字段名
            target_field: 目标字段名
            
        Returns:
            Dict: 统计信息
        """
        print("\n" + "=" * 70)
        print("批量提取最终得分")
        print("=" * 70)
        
        stats = {
            "total": len(record_ids),
            "success": 0,
            "failed": 0,
            "scores": []
        }
        
        for idx, record_id in enumerate(record_ids):
            print(f"\n[{idx+1}/{len(record_ids)}] 处理记录: {record_id}")
            
            score = self.process_record(record_id, source_field, target_field)
            
            if score is not None:
                stats["success"] += 1
                stats["scores"].append(score)
            else:
                stats["failed"] += 1
        
        print("\n" + "=" * 70)
        print("📊 批量处理完成")
        print("=" * 70)
        print(f"总数: {stats['total']}")
        print(f"成功: {stats['success']}")
        print(f"失败: {stats['failed']}")
        
        if stats["scores"]:
            avg_score = sum(stats["scores"]) / len(stats["scores"])
            print(f"平均得分: {avg_score:.2f}")
        
        return stats


# ============================================================================
# 便捷函数
# ============================================================================

def extract_and_update_scores(
    record_ids: List[str],
    source_field: str = "视频质量评分",
    target_field: str = "最终得分",
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    table_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    便捷函数：批量提取并更新得分
    
    Args:
        record_ids: 记录 ID 列表
        source_field: 源字段名（大模型分析结果）
        target_field: 目标字段名（最终得分）
        app_id: 飞书应用 ID
        app_secret: 飞书应用密钥
        table_id: 多维表格 ID
        
    Returns:
        Dict: 统计信息
        
    Example:
        stats = extract_and_update_scores(
            record_ids=["rec001", "rec002", "rec003"],
            source_field="视频质量评分",
            target_field="最终得分"
        )
    """
    extractor = ScoreExtractor(app_id, app_secret, table_id)
    return extractor.process_all_records(record_ids, source_field, target_field)


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 测试提取功能
    
    # 测试用例 1: 标准格式
    test_text_1 = '''```json
{
  "final_star_rating": 4.5,
  "content_quality": 4.0,
  "visual_appeal": 5.0,
  "engagement": 4.5,
  "analysis": "视频质量优秀"
}
```'''
    
    # 测试用例 2: 无标记
    test_text_2 = '''
{
  "final_score": 3.8,
  "details": "..."
}
'''
    
    # 测试用例 3: 多余空白
    test_text_3 = '''
    
    ```json
    
    {
      "total_score": 4.2
    }
    
    ```
    
    '''
    
    extractor = ScoreExtractor()
    
    print("测试用例 1:")
    score1 = extractor.extract_final_score(test_text_1)
    print(f"提取得分: {score1}\n")
    
    print("测试用例 2:")
    score2 = extractor.extract_final_score(test_text_2)
    print(f"提取得分: {score2}\n")
    
    print("测试用例 3:")
    score3 = extractor.extract_final_score(test_text_3)
    print(f"提取得分: {score3}\n")
    
    # 批量处理测试
    print("\n批量处理测试:")
    stats = extract_and_update_scores(
        record_ids=["rec001", "rec002", "rec003"],
        source_field="视频质量评分",
        target_field="最终得分"
    )
    
    print(f"\n统计: {stats}")
