#!/usr/bin/env python3
"""
飞书文件消息处理模块
自动读取飞书群聊中的文件附件
"""

import os
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any

# 导入文件读取模块
from file_reader import read_file, format_file_summary


def find_feishu_media_files() -> list:
    """
    查找飞书下载的媒体文件
    飞书文件通常保存在 /tmp/openclaw/ 或 ~/.openclaw/tmp/ 目录
    """
    media_files = []
    
    # 可能的临时目录
    temp_dirs = [
        os.path.expanduser("~/.openclaw/media/inbound"),
        "/tmp/openclaw",
        os.path.expanduser("~/.openclaw/tmp"),
        "/tmp",
    ]
    
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            # 查找最近下载的文件（1小时内）
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # 检查文件扩展名
                    if file.lower().endswith(('.xlsx', '.xls', '.csv', '.txt', '.json')):
                        # 检查文件修改时间（1小时内）
                        try:
                            mtime = os.path.getmtime(file_path)
                            import time
                            if time.time() - mtime < 3600:  # 1小时内
                                media_files.append({
                                    "path": file_path,
                                    "name": file,
                                    "mtime": mtime
                                })
                        except:
                            pass
    
    # 按修改时间排序，最新的在前
    media_files.sort(key=lambda x: x["mtime"], reverse=True)
    return media_files


def get_latest_feishu_file() -> Optional[Dict[str, Any]]:
    """获取最新的飞书文件"""
    files = find_feishu_media_files()
    return files[0] if files else None


def process_feishu_file(file_path: Optional[str] = None) -> str:
    """
    处理飞书文件
    
    Args:
        file_path: 文件路径，如果为 None 则自动查找最新的文件
    
    Returns:
        文件内容摘要
    """
    if file_path is None:
        # 自动查找最新的文件
        latest_file = get_latest_feishu_file()
        if latest_file is None:
            return "❌ 未找到飞书文件。请确保文件已发送并被下载。"
        file_path = latest_file["path"]
    
    if not os.path.exists(file_path):
        return f"❌ 文件不存在: {file_path}"
    
    # 读取文件
    result = read_file(file_path)
    
    # 格式化输出
    return format_file_summary(result)


def get_file_data(file_path: Optional[str] = None) -> Dict[str, Any]:
    """
    获取文件数据（用于数据分析）
    
    Args:
        file_path: 文件路径，如果为 None 则自动查找最新的文件
    
    Returns:
        文件数据字典
    """
    if file_path is None:
        latest_file = get_latest_feishu_file()
        if latest_file is None:
            return {"success": False, "error": "未找到文件"}
        file_path = latest_file["path"]
    
    return read_file(file_path)


if __name__ == "__main__":
    # 命令行用法
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(process_feishu_file(file_path))
    else:
        # 自动查找并处理最新的文件
        print(process_feishu_file())
