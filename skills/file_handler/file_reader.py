#!/usr/bin/env python3
"""
文件读取模块 - 支持多种格式的文件读取
"""

import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List


def read_excel(file_path: str, max_rows: int = 100) -> Dict[str, Any]:
    """读取 Excel 文件"""
    try:
        df = pd.read_excel(file_path)
        return {
            "success": True,
            "type": "excel",
            "file_name": os.path.basename(file_path),
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "preview": df.head(max_rows).to_string(),
            "sample_data": df.head(5).to_dict(orient='records')
        }
    except Exception as e:
        return {
            "success": False,
            "type": "excel",
            "file_name": os.path.basename(file_path),
            "error": str(e)
        }


def read_csv(file_path: str, max_rows: int = 100) -> Dict[str, Any]:
    """读取 CSV 文件"""
    try:
        df = pd.read_csv(file_path)
        return {
            "success": True,
            "type": "csv",
            "file_name": os.path.basename(file_path),
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "preview": df.head(max_rows).to_string(),
            "sample_data": df.head(5).to_dict(orient='records')
        }
    except Exception as e:
        return {
            "success": False,
            "type": "csv",
            "file_name": os.path.basename(file_path),
            "error": str(e)
        }


def read_text(file_path: str, max_chars: int = 5000) -> Dict[str, Any]:
    """读取文本文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read(max_chars)
        return {
            "success": True,
            "type": "text",
            "file_name": os.path.basename(file_path),
            "size": os.path.getsize(file_path),
            "preview": content[:max_chars]
        }
    except Exception as e:
        return {
            "success": False,
            "type": "text",
            "file_name": os.path.basename(file_path),
            "error": str(e)
        }


def read_json(file_path: str, max_items: int = 100) -> Dict[str, Any]:
    """读取 JSON 文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 如果是列表，只显示前 max_items 个
        if isinstance(data, list):
            preview = data[:max_items]
            item_count = len(data)
        else:
            preview = data
            item_count = 1
        
        return {
            "success": True,
            "type": "json",
            "file_name": os.path.basename(file_path),
            "item_count": item_count,
            "preview": json.dumps(preview, ensure_ascii=False, indent=2)[:5000]
        }
    except Exception as e:
        return {
            "success": False,
            "type": "json",
            "file_name": os.path.basename(file_path),
            "error": str(e)
        }


def detect_file_type(file_path: str) -> str:
    """检测文件类型"""
    ext = Path(file_path).suffix.lower()
    
    if ext in ['.xlsx', '.xls']:
        return 'excel'
    elif ext == '.csv':
        return 'csv'
    elif ext == '.json':
        return 'json'
    elif ext in ['.txt', '.md', '.py', '.js', '.ts', '.html', '.css']:
        return 'text'
    else:
        return 'unknown'


def read_file(file_path: str, **kwargs) -> Dict[str, Any]:
    """
    通用文件读取函数
    
    Args:
        file_path: 文件路径
        **kwargs: 额外参数
    
    Returns:
        文件内容字典
    """
    if not os.path.exists(file_path):
        return {
            "success": False,
            "error": f"文件不存在: {file_path}"
        }
    
    file_type = detect_file_type(file_path)
    
    if file_type == 'excel':
        return read_excel(file_path, **kwargs)
    elif file_type == 'csv':
        return read_csv(file_path, **kwargs)
    elif file_type == 'json':
        return read_json(file_path, **kwargs)
    elif file_type == 'text':
        return read_text(file_path, **kwargs)
    else:
        return {
            "success": False,
            "type": "unknown",
            "file_name": os.path.basename(file_path),
            "error": f"不支持的文件类型: {file_type}"
        }


def format_file_summary(result: Dict[str, Any]) -> str:
    """格式化文件摘要"""
    if not result.get("success"):
        return f"❌ 读取失败: {result.get('error', '未知错误')}"
    
    file_type = result.get("type")
    file_name = result.get("file_name", "未知文件")
    
    lines = [f"📄 **{file_name}**"]
    
    if file_type in ["excel", "csv"]:
        lines.append(f"- 类型: {file_type.upper()}")
        lines.append(f"- 行数: {result.get('rows', 'N/A')}")
        lines.append(f"- 列数: {result.get('columns', 'N/A')}")
        lines.append(f"- 列名: {', '.join(str(c) for c in result.get('column_names', []))}")
        lines.append("")
        lines.append("**预览（前5行）:**")
        lines.append("```")
        lines.append(result.get("preview", "")[:1000])
        lines.append("```")
    
    elif file_type == "json":
        lines.append(f"- 类型: JSON")
        lines.append(f"- 项目数: {result.get('item_count', 'N/A')}")
        lines.append("")
        lines.append("**预览:**")
        lines.append("```json")
        lines.append(result.get("preview", "")[:1000])
        lines.append("```")
    
    elif file_type == "text":
        lines.append(f"- 类型: 文本文件")
        lines.append(f"- 大小: {result.get('size', 'N/A')} bytes")
        lines.append("")
        lines.append("**预览:**")
        lines.append("```")
        lines.append(result.get("preview", "")[:1000])
        lines.append("```")
    
    return "\n".join(lines)


if __name__ == "__main__":
    # 测试代码
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        result = read_file(file_path)
        print(format_file_summary(result))
    else:
        print("用法: python file_reader.py <文件路径>")
