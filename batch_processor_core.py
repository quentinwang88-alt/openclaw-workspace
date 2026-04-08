#!/usr/bin/env python3
"""
批处理核心模块 - 消除重复代码
处理达人视频封面的通用流程
"""
import sys
import json
from pathlib import Path
from typing import List, Dict, Any

from workspace_support import REPO_ROOT

# 添加路径
workspace_root = REPO_ROOT
sys.path.insert(0, str(workspace_root))
sys.path.insert(0, str(workspace_root / "skills/creator-crm"))

from core.data_fetchers import OEmbedFetcher
from core.image_processor import GridCanvasGenerator
from core.sub_agents import FeishuUploaderAgent

# 直接从工作区根目录导入 config.py
import importlib.util
config_path = workspace_root / "config.py"
spec = importlib.util.spec_from_file_location("workspace_config", config_path)
workspace_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workspace_config)

APP_TOKEN = workspace_config.FEISHU_APP_TOKEN
TABLE_ID = workspace_config.FEISHU_TABLE_ID
MIN_COVER_COUNT = workspace_config.MIN_COVER_COUNT

# 每个达人最多获取的视频数（生成两张宫图需要 24 个）
MAX_VIDEOS_PER_CREATOR = 24

OUTPUT_DIR = workspace_root / "output" / "grids"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def process_creator(creator_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理单个达人：获取前 24 个视频封面（如果不足 24 个则有多少抓多少），生成两张宫图并分别上传飞书

    - 宫图 1：第 1-12 张封面
    - 宫图 2：第 13-24 张封面（如果封面不足 12 张则跳过）
    - 封面上包含视频播放量和带货金额数据

    Args:
        creator_data: 达人数据字典，包含 record_id, tk_handle, video_ids, video_data (可选)

    Returns:
        处理结果字典
    """
    tk_handle = creator_data['tk_handle']
    record_id = creator_data['record_id']
    video_ids = creator_data['video_ids']
    video_data = creator_data.get('video_data', [])  # 包含播放量和带货金额的完整视频数据

    print(f"\n{'='*70}")
    print(f"处理达人: {tk_handle}")
    print(f"Record ID: {record_id}")
    print(f"视频数量: {len(video_ids)}（抓取前 {min(len(video_ids), MAX_VIDEOS_PER_CREATOR)} 个）")
    print(f"{'='*70}")

    try:
        # 步骤 1: 获取封面（有多少抓多少，最多 24 个）
        fetch_count = min(len(video_ids), MAX_VIDEOS_PER_CREATOR)
        print(f"⏳ 步骤 1/3: 获取封面（共 {fetch_count} 个）...")

        fetcher = OEmbedFetcher(cache_enabled=True, timeout=10)
        cover_urls = []
        views_list = []
        revenue_list = []

        for i, vid in enumerate(video_ids[:fetch_count], 1):
            print(f"  [{i}/{fetch_count}] 获取视频 {vid}...", end=" ")
            url = fetcher.fetch_cover_url(tk_handle, vid)
            if url:
                cover_urls.append(url)
                # 尝试从 video_data 中获取播放量和带货金额
                if video_data and i-1 < len(video_data):
                    views_list.append(video_data[i-1].get('views', 0))
                    revenue_list.append(video_data[i-1].get('revenue', 0.0))
                else:
                    views_list.append(0)
                    revenue_list.append(0.0)
                print("✅")
            else:
                print("❌")

        print(f"  封面获取结果: {len(cover_urls)}/{fetch_count}")

        if len(cover_urls) < MIN_COVER_COUNT:
            print(f"❌ 封面不足（需要至少 {MIN_COVER_COUNT} 个，实际 {len(cover_urls)} 个）")
            return {
                'status': 'failed',
                'reason': 'insufficient_covers',
                'tk_handle': tk_handle
            }

        # 步骤 2: 生成宫图（生成两张，每张 12 个）
        print(f"⏳ 步骤 2/3: 生成宫图（共 {len(cover_urls)} 个封面）...")
        generator = GridCanvasGenerator()

        # 分批处理：第一批 12 张，第二批剩余的（如果有）
        batch1_urls = cover_urls[:12]
        batch1_views = views_list[:12]
        batch1_revenue = revenue_list[:12]
        
        batch2_urls = cover_urls[12:24] if len(cover_urls) > 12 else []
        batch2_views = views_list[12:24] if len(views_list) > 12 else []
        batch2_revenue = revenue_list[12:24] if len(revenue_list) > 12 else []

        grid_paths = []

        # 处理第一张宫图
        batches = [
            (1, batch1_urls, batch1_views, batch1_revenue),
            (2, batch2_urls, batch2_views, batch2_revenue)
        ]

        for batch_idx, batch_urls, batch_views, batch_revenue in batches:
            # 第二张宫图需要至少 MIN_COVER_COUNT 个封面
            if not batch_urls or (batch_idx == 2 and len(batch_urls) < MIN_COVER_COUNT):
                print(f"  ⏭️ 宫图 {batch_idx}：封面不足（需要 {MIN_COVER_COUNT} 个，实际 {len(batch_urls)} 个），跳过")
                continue

            print(f"  下载图片（宫图 {batch_idx}，共 {len(batch_urls)} 张）...")
            images = generator.downloader.download_images_batch(batch_urls)
            print(f"  下载结果: {len(images)}/{len(batch_urls)}")

            if len(images) < MIN_COVER_COUNT:
                print(f"  ⚠️ 宫图 {batch_idx}：下载成功图片不足 {MIN_COVER_COUNT} 张，跳过")
                continue

            print(f"  创建宫格 {batch_idx}（包含播放量和带货金额）...")
            # 传入播放量和带货金额数据
            canvas = generator.create_canvas(
                images,
                max_images=12,
                views_list=batch_views[:len(images)],
                revenue_list=batch_revenue[:len(images)]
            )
            output_path = OUTPUT_DIR / f"{tk_handle}_grid_{batch_idx}.jpg"
            canvas.save(output_path, format='JPEG', quality=85)
            print(f"  ✅ 宫图 {batch_idx} 已生成: {output_path}")
            grid_paths.append(str(output_path))

        if not grid_paths:
            print("❌ 所有宫图生成失败")
            return {
                'status': 'failed',
                'reason': 'all_grids_failed',
                'tk_handle': tk_handle
            }

        # 步骤 3: 上传到飞书
        print(f"⏳ 步骤 3/3: 上传到飞书（共 {len(grid_paths)} 张宫图）...")
        uploader = FeishuUploaderAgent()
        file_tokens = []

        for grid_path in grid_paths:
            upload_result = uploader.execute({
                'tk_handle': tk_handle,
                'grid_path': grid_path,
                'record_id': record_id,
                'app_token': APP_TOKEN,
                'table_id': TABLE_ID
            })
            file_tokens.append(upload_result['file_token'])
            print(f"  ✅ 上传成功: {upload_result['file_token']} ({Path(grid_path).name})")

        return {
            'status': 'success',
            'tk_handle': tk_handle,
            'record_id': record_id,
            'grid_paths': grid_paths,
            'file_tokens': file_tokens,
            'grids_generated': len(grid_paths)
        }

    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'failed',
            'reason': str(e),
            'tk_handle': tk_handle
        }


def run_batch(creators: List[Dict[str, Any]], batch_name: str = "批次") -> Dict[str, Any]:
    """
    运行批处理流程

    Args:
        creators: 达人列表
        batch_name: 批次名称（用于显示）

    Returns:
        批处理结果统计
    """
    print("=" * 70)
    print(f"批量处理达人 ({batch_name})")
    print("=" * 70)
    print(f"待处理达人数量: {len(creators)}\n")

    results = []
    success_count = 0
    failed_count = 0

    for i, creator in enumerate(creators, 1):
        print(f"\n[{i}/{len(creators)}] 开始处理...")
        result = process_creator(creator)
        results.append(result)

        if result['status'] == 'success':
            success_count += 1
        else:
            failed_count += 1

    # 打印汇总
    print("\n" + "=" * 70)
    print("批处理完成")
    print("=" * 70)
    print(f"✅ 成功: {success_count}")
    print(f"❌ 失败: {failed_count}")
    print(f"📊 总计: {len(creators)}")

    # 保存结果
    summary = {
        'batch_name': batch_name,
        'total': len(creators),
        'success': success_count,
        'failed': failed_count,
        'results': results
    }

    summary_file = OUTPUT_DIR / f"batch_{batch_name.replace(' ', '_')}_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"\n📄 结果已保存: {summary_file}")

    return summary


if __name__ == "__main__":
    print("这是批处理核心模块，请使用 batch_runner.py 运行批处理任务")
