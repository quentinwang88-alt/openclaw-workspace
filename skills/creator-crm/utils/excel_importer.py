#!/usr/bin/env python3
"""
Excel 导入器 - 从 Excel 读取达人数据并写入飞书多维表格

功能：
1. 读取 Excel 文件
2. 字段映射与验证
3. 批量写入飞书多维表格
"""

import sys
import os
from typing import List, Dict, Any, Optional
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("⚠️ pandas 未安装，请运行: pip install pandas openpyxl")


class ExcelImporter:
    """Excel 导入器"""
    
    def __init__(
        self,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        table_id: Optional[str] = None
    ):
        """
        初始化 Excel 导入器
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
            table_id: 多维表格 ID
        """
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        self.table_id = table_id or os.getenv("FEISHU_TABLE_ID")
        
        if not PANDAS_AVAILABLE:
            raise ImportError("需要安装 pandas: pip install pandas openpyxl")
    
    def read_excel(
        self,
        file_path: str,
        sheet_name: str = 0
    ) -> pd.DataFrame:
        """
        读取 Excel 文件
        
        Args:
            file_path: Excel 文件路径
            sheet_name: 工作表名称或索引（默认第一个）
            
        Returns:
            pd.DataFrame: 数据框
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 文件格式错误
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Excel 文件不存在: {file_path}")
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"✅ 读取 Excel 成功: {len(df)} 行数据")
            return df
        except Exception as e:
            raise ValueError(f"读取 Excel 失败: {str(e)}")
    
    def map_fields(
        self,
        df: pd.DataFrame,
        field_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        字段映射（优化版本 - 使用向量化操作）
        
        Args:
            df: 数据框
            field_mapping: 字段映射字典 {Excel列名: 飞书字段名}
            
        Returns:
            List[Dict]: 映射后的数据列表
            
        Example:
            field_mapping = {
                "达人账号": "tk_handle",
                "主页链接": "tk_profile_url",
                "粉丝数": "followers_count"
            }
        """
        # 只保留需要的列
        available_cols = [col for col in field_mapping.keys() if col in df.columns]
        missing_cols = set(field_mapping.keys()) - set(available_cols)
        
        if missing_cols:
            print(f"⚠️ 缺失列: {missing_cols}")
        
        # 创建映射后的 DataFrame
        df_mapped = df[available_cols].copy()
        df_mapped.columns = [field_mapping[col] for col in available_cols]
        
        # 处理 NaN 值
        df_mapped = df_mapped.where(pd.notna(df_mapped), None)
        
        # 转换为字典列表（比 iterrows 快 10-100 倍）
        records = df_mapped.to_dict('records')
        
        print(f"✅ 字段映射完成: {len(records)} 条记录")
        return records
    
    def validate_records(
        self,
        records: List[Dict[str, Any]],
        required_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        验证记录（优化版本 - 批量处理）
        
        Args:
            records: 记录列表
            required_fields: 必填字段列表
            
        Returns:
            List[Dict]: 验证通过的记录
        """
        valid_records = []
        invalid_count = 0
        
        for record in records:
            # 检查必填字段（使用 all() 更高效）
            if all(record.get(f) for f in required_fields):
                valid_records.append(record)
            else:
                invalid_count += 1
        
        if invalid_count > 0:
            print(f"⚠️ 跳过 {invalid_count} 条无效记录")
        
        print(f"✅ 验证完成: {len(valid_records)}/{len(records)} 条记录有效")
        return valid_records
    
    def write_to_feishu(
        self,
        records: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        批量写入飞书多维表格
        
        Args:
            records: 记录列表
            batch_size: 批量大小
            
        Returns:
            Dict: 统计信息 {"success": 成功数, "failed": 失败数}
        """
        if not all([self.app_id, self.app_secret, self.table_id]):
            print("⚠️ 飞书配置未完整，使用模拟模式")
            return {"success": len(records), "failed": 0}
        
        # TODO: 实际的飞书 API 调用
        # 这里需要使用飞书 SDK 或 HTTP API
        # 示例代码：
        # import lark_oapi as lark
        # client = lark.Client.builder().app_id(self.app_id).app_secret(self.app_secret).build()
        # 
        # success_count = 0
        # failed_count = 0
        # 
        # for i in range(0, len(records), batch_size):
        #     batch = records[i:i+batch_size]
        #     try:
        #         response = client.bitable.v1.app_table_record.batch_create(...)
        #         success_count += len(batch)
        #     except Exception as e:
        #         print(f"❌ 批次 {i//batch_size + 1} 写入失败: {str(e)}")
        #         failed_count += len(batch)
        
        # 模拟写入
        print(f"📝 模拟写入 {len(records)} 条记录到飞书表格...")
        
        return {
            "success": len(records),
            "failed": 0
        }
    
    def import_from_excel(
        self,
        file_path: str,
        field_mapping: Dict[str, str],
        required_fields: Optional[List[str]] = None,
        sheet_name: str = 0
    ) -> Dict[str, int]:
        """
        完整导入流程
        
        Args:
            file_path: Excel 文件路径
            field_mapping: 字段映射字典
            required_fields: 必填字段列表
            sheet_name: 工作表名称或索引
            
        Returns:
            Dict: 统计信息
        """
        print("🚀 开始导入 Excel 数据到飞书")
        print("=" * 70)
        
        # 1. 读取 Excel
        df = self.read_excel(file_path, sheet_name)
        
        # 2. 字段映射
        records = self.map_fields(df, field_mapping)
        
        # 3. 验证记录
        if required_fields:
            records = self.validate_records(records, required_fields)
        
        # 4. 写入飞书
        stats = self.write_to_feishu(records)
        
        print("=" * 70)
        print(f"✅ 导入完成: 成功 {stats['success']} 条，失败 {stats['failed']} 条")
        
        return stats


# ============================================================================
# 便捷函数
# ============================================================================

def import_excel_to_feishu(
    file_path: str,
    field_mapping: Dict[str, str],
    required_fields: Optional[List[str]] = None,
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    table_id: Optional[str] = None
) -> Dict[str, int]:
    """
    便捷函数：从 Excel 导入到飞书
    
    Args:
        file_path: Excel 文件路径
        field_mapping: 字段映射字典
        required_fields: 必填字段列表
        app_id: 飞书应用 ID
        app_secret: 飞书应用密钥
        table_id: 多维表格 ID
        
    Returns:
        Dict: 统计信息
        
    Example:
        stats = import_excel_to_feishu(
            file_path="达人列表.xlsx",
            field_mapping={
                "达人账号": "tk_handle",
                "主页链接": "tk_profile_url",
                "粉丝数": "followers_count"
            },
            required_fields=["tk_handle", "tk_profile_url"]
        )
    """
    importer = ExcelImporter(app_id, app_secret, table_id)
    return importer.import_from_excel(
        file_path,
        field_mapping,
        required_fields
    )


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 示例：导入 Excel 到飞书
    
    # 字段映射配置
    field_mapping = {
        "达人账号": "tk_handle",
        "主页链接": "tk_profile_url",
        "粉丝数": "followers_count",
        "联系方式": "contact_account"
    }
    
    # 必填字段
    required_fields = ["tk_handle", "tk_profile_url"]
    
    # 执行导入
    try:
        stats = import_excel_to_feishu(
            file_path="达人列表.xlsx",
            field_mapping=field_mapping,
            required_fields=required_fields
        )
        
        print(f"\n📊 导入统计:")
        print(f"  成功: {stats['success']} 条")
        print(f"  失败: {stats['failed']} 条")
        
    except Exception as e:
        print(f"❌ 导入失败: {str(e)}")
