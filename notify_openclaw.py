#!/usr/bin/env python3
"""
OpenClaw 通知接口 - 触发流水线处理未处理的记录

使用方法：
    # 方式 1: 直接运行（推荐）
    python3 notify_openclaw.py
    
    # 方式 2: 指定处理数量
    python3 notify_openclaw.py --limit 10
    
    # 方式 3: 通过 HTTP API 触发
    curl -X POST http://localhost:8766/run
"""

import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

from workspace_support import REPO_ROOT

# 添加路径
workspace_root = REPO_ROOT
sys.path.insert(0, str(workspace_root))


def notify_openclaw_direct(limit: int = None):
    """
    直接通知 OpenClaw 处理未处理的记录
    
    Args:
        limit: 限制处理数量（可选）
    """
    print("="*70)
    print("通知 OpenClaw 处理未处理的记录")
    print("="*70)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if limit:
        print(f"限制处理数量: {limit}")
    
    print("\n开始运行流水线...")
    print("-"*70)
    
    # 构建命令
    cmd = ["python3", str(workspace_root / "creator_grid_pipeline.py"), "run"]
    if limit:
        cmd.extend(["--limit", str(limit)])
    
    # 运行流水线
    try:
        result = subprocess.run(
            cmd,
            cwd=str(workspace_root),
            capture_output=True,
            text=True
        )
        
        # 打印输出
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("错误输出:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
        
        if result.returncode == 0:
            print("\n" + "="*70)
            print("✅ 流水线运行完成")
            print("="*70)
        else:
            print("\n" + "="*70)
            print("❌ 流水线运行失败")
            print("="*70)
            sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ 运行失败: {e}", file=sys.stderr)
        sys.exit(1)


def notify_openclaw_http(limit: int = None):
    """
    通过 HTTP API 通知 OpenClaw
    
    Args:
        limit: 限制处理数量（可选）
    """
    import requests
    
    print("="*70)
    print("通过 HTTP API 通知 OpenClaw")
    print("="*70)
    
    url = "http://localhost:8766/run"
    payload = {}
    
    if limit:
        payload['limit'] = limit
    
    try:
        print(f"发送请求到: {url}")
        if payload:
            print(f"参数: {payload}")
        
        response = requests.post(url, json=payload, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        
        print("\n" + "="*70)
        print("运行结果")
        print("="*70)
        print(f"运行 ID: {result.get('run_id')}")
        print(f"开始时间: {result.get('started_at')}")
        print(f"结束时间: {result.get('completed_at')}")
        print(f"总计: {result.get('total')}")
        print(f"成功: {result.get('success')}")
        print(f"失败: {result.get('failed')}")
        
        if result.get('success', 0) > 0:
            print("\n✅ 流水线运行完成")
        else:
            print("\n⚠️  没有成功处理的记录")
    
    except requests.exceptions.ConnectionError:
        print("\n❌ 无法连接到 HTTP 服务")
        print("请先启动服务: python3 creator_grid_pipeline.py serve")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ 请求失败: {e}", file=sys.stderr)
        sys.exit(1)


def check_service_status():
    """检查 HTTP 服务状态"""
    import requests
    
    try:
        response = requests.get("http://localhost:8766/health", timeout=2)
        return response.status_code == 200
    except:
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='通知 OpenClaw 处理未处理的记录',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 处理所有未处理的记录
  python3 notify_openclaw.py
  
  # 处理前 10 个未处理的记录
  python3 notify_openclaw.py --limit 10
  
  # 使用 HTTP API（需要先启动服务）
  python3 notify_openclaw.py --http
  
  # 检查服务状态
  python3 notify_openclaw.py --check
        """
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='限制处理数量（用于测试）'
    )
    
    parser.add_argument(
        '--http',
        action='store_true',
        help='使用 HTTP API 触发（需要先启动服务）'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='检查 HTTP 服务状态'
    )
    
    args = parser.parse_args()
    
    # 检查服务状态
    if args.check:
        print("检查 HTTP 服务状态...")
        if check_service_status():
            print("✅ HTTP 服务正在运行")
            print("地址: http://localhost:8766")
        else:
            print("❌ HTTP 服务未运行")
            print("启动服务: python3 creator_grid_pipeline.py serve")
        return
    
    # 选择触发方式
    if args.http:
        # 使用 HTTP API
        notify_openclaw_http(limit=args.limit)
    else:
        # 直接运行
        notify_openclaw_direct(limit=args.limit)


if __name__ == "__main__":
    main()
