#!/usr/bin/env python3
"""
为飞书多维表格中的达人记录补充视频封面
"""
import os
import json
import subprocess
import time
import re

from workspace_support import load_repo_env

load_repo_env()

# 飞书表格配置
APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
TABLE_ID = os.environ.get("FEISHU_TABLE_ID", "")

def get_tiktok_handle(url):
    """从URL中提取TikTok handle"""
    match = re.search(r'tiktok\.com/@([^/?]+)', url)
    return match.group(1) if match else None

def fetch_video_cover(handle):
    """获取TikTok用户的最新视频封面"""
    # 使用TikTok API或网页抓取获取封面
    # 这里需要实现具体的抓取逻辑
    print(f"  获取 @{handle} 的视频封面...")
    # TODO: 实现封面获取逻辑
    return None

def upload_to_feishu(image_path, record_id):
    """上传图片到飞书并更新记录"""
    print(f"  上传封面到记录 {record_id}...")
    # TODO: 调用飞书API上传
    pass

def main():
    print("开始补充视频封面...")
    
    # 获取所有记录
    cmd = f'openclaw feishu_bitable_list_records --app_token {APP_TOKEN} --table_id {TABLE_ID} --page_size 100'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"获取记录失败: {result.stderr}")
        return
    
    data = json.loads(result.stdout)
    records = data.get('records', [])
    
    # 筛选需要补充封面的记录
    missing_covers = []
    for record in records:
        fields = record.get('fields', {})
        if '视频截图' not in fields or not fields['视频截图']:
            url = fields.get('Kalodata_URL', {}).get('link', '')
            handle = get_tiktok_handle(url)
            if handle:
                missing_covers.append({
                    'record_id': record['record_id'],
                    'handle': handle,
                    'name': fields.get('达人名称', '')
                })
    
    print(f"\n找到 {len(missing_covers)} 条需要补充封面的记录")
    
    # 处理每条记录
    for i, record in enumerate(missing_covers[:10], 1):  # 先处理前10条
        print(f"\n[{i}/{min(10, len(missing_covers))}] {record['name']} (@{record['handle']})")
        
        # 获取封面
        cover_path = fetch_video_cover(record['handle'])
        if cover_path:
            # 上传到飞书
            upload_to_feishu(cover_path, record['record_id'])
            time.sleep(1)  # 避免请求过快
        else:
            print(f"  ⚠️  无法获取封面")
    
    print("\n✅ 完成")

if __name__ == '__main__':
    main()
