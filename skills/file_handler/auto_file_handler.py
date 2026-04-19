#!/usr/bin/env python3
"""
自动文件处理器 - 检测并读取飞书文件消息

使用方法:
1. 在飞书群聊中发送文件
2. 运行此脚本自动读取文件内容
3. 或导入使用 auto_read_feishu_file() 函数
"""

import os
import sys
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, List

# 导入文件读取模块
from file_reader import read_file, format_file_summary


class FeishuFileWatcher:
    """飞书文件监视器 - 检测新文件"""
    
    def __init__(self, watch_dirs: List[str] = None):
        """
        初始化文件监视器
        
        Args:
            watch_dirs: 监视的目录列表，默认为常用临时目录
        """
        if watch_dirs is None:
            self.watch_dirs = [
                os.path.expanduser("~/.openclaw/media/inbound"),
                "/tmp/openclaw",
                os.path.expanduser("~/.openclaw/tmp"),
                "/tmp",
            ]
        else:
            self.watch_dirs = watch_dirs
        
        self.known_files = set()
        self._init_known_files()
    
    def _init_known_files(self):
        """初始化已知文件列表"""
        for watch_dir in self.watch_dirs:
            if os.path.exists(watch_dir):
                for root, dirs, files in os.walk(watch_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        self.known_files.add(file_path)
    
    def find_new_files(self, max_age_seconds: int = 300) -> List[Dict[str, Any]]:
        """
        查找新文件
        
        Args:
            max_age_seconds: 最大文件年龄（秒），默认5分钟
        
        Returns:
            新文件列表
        """
        new_files = []
        current_time = time.time()
        
        for watch_dir in self.watch_dirs:
            if not os.path.exists(watch_dir):
                continue
            
            for root, dirs, files in os.walk(watch_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # 跳过已知文件
                    if file_path in self.known_files:
                        continue
                    
                    # 检查文件扩展名
                    if not file.lower().endswith(('.xlsx', '.xls', '.csv', '.txt', '.json')):
                        continue
                    
                    # 检查文件修改时间
                    try:
                        mtime = os.path.getmtime(file_path)
                        age = current_time - mtime
                        
                        if age < max_age_seconds:
                            new_files.append({
                                "path": file_path,
                                "name": file,
                                "mtime": mtime,
                                "age_seconds": age
                            })
                            self.known_files.add(file_path)
                    except:
                        pass
        
        # 按修改时间排序，最新的在前
        new_files.sort(key=lambda x: x["mtime"], reverse=True)
        return new_files
    
    def wait_for_new_file(self, timeout_seconds: int = 60) -> Optional[Dict[str, Any]]:
        """
        等待新文件
        
        Args:
            timeout_seconds: 超时时间（秒）
        
        Returns:
            新文件信息或 None
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            new_files = self.find_new_files()
            if new_files:
                return new_files[0]
            time.sleep(1)
        
        return None


def auto_read_feishu_file(file_path: Optional[str] = None, 
                          wait_for_new: bool = False,
                          timeout: int = 60) -> Dict[str, Any]:
    """
    自动读取飞书文件
    
    Args:
        file_path: 指定文件路径，如果为 None 则自动查找
        wait_for_new: 是否等待新文件
        timeout: 等待超时时间（秒）
    
    Returns:
        包含文件信息的字典
    """
    if file_path:
        # 读取指定文件
        result = read_file(file_path)
        return {
            "success": result.get("success", False),
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "data": result,
            "summary": format_file_summary(result)
        }
    
    if wait_for_new:
        # 等待新文件
        watcher = FeishuFileWatcher()
        print(f"⏳ 等待新文件...（超时: {timeout}秒）")
        new_file = watcher.wait_for_new_file(timeout)
        
        if new_file is None:
            return {
                "success": False,
                "error": "等待超时，未检测到新文件"
            }
        
        file_path = new_file["path"]
        result = read_file(file_path)
        return {
            "success": True,
            "file_path": file_path,
            "file_name": new_file["name"],
            "data": result,
            "summary": format_file_summary(result),
            "wait_time": timeout
        }
    else:
        # 查找最新的现有文件
        watcher = FeishuFileWatcher()
        new_files = watcher.find_new_files(max_age_seconds=3600)  # 1小时内
        
        if not new_files:
            return {
                "success": False,
                "error": "未找到文件"
            }
        
        latest_file = new_files[0]
        file_path = latest_file["path"]
        result = read_file(file_path)
        
        return {
            "success": True,
            "file_path": file_path,
            "file_name": latest_file["name"],
            "data": result,
            "summary": format_file_summary(result),
            "file_age_seconds": latest_file.get("age_seconds", 0)
        }


def main():
    """主函数 - 命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="飞书文件自动读取器")
    parser.add_argument("--file", "-f", help="指定文件路径")
    parser.add_argument("--wait", "-w", action="store_true", help="等待新文件")
    parser.add_argument("--timeout", "-t", type=int, default=60, help="等待超时（秒）")
    parser.add_argument("--json", "-j", action="store_true", help="输出JSON格式")
    
    args = parser.parse_args()
    
    # 执行读取
    result = auto_read_feishu_file(
        file_path=args.file,
        wait_for_new=args.wait,
        timeout=args.timeout
    )
    
    # 输出结果
    if args.json:
        # JSON 格式输出
        output = {
            "success": result.get("success"),
            "file_path": result.get("file_path"),
            "file_name": result.get("file_name"),
            "data": result.get("data")
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        # 文本格式输出
        if result.get("success"):
            print(result.get("summary", ""))
        else:
            print(f"❌ {result.get('error', '未知错误')}")


if __name__ == "__main__":
    main()
