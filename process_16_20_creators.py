#!/usr/bin/env python3
"""
处理第16-20个达人 - 生成两张宫图（各12张封面）并上传飞书
每个达人获取最多24个视频封面，拼成两张 3x4 宫格图
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path.home() / ".openclaw/workspace/skills/creator-crm"))

from core.data_fetchers import OEmbedFetcher
from core.image_processor import GridCanvasGenerator
from core.sub_agents import FeishuUploaderAgent
from config import FEISHU_APP_TOKEN as APP_TOKEN, FEISHU_TABLE_ID as TABLE_ID, MIN_COVER_COUNT

# 每个达人最多获取的视频数（生成两张宫图需要 24 个）
MAX_VIDEOS_PER_CREATOR = 24

# 第16-20个达人的数据
CREATORS = [
    {
        "record_id": "recvdmfnKasUKn",
        "tk_handle": "thananya4506",
        "video_ids": [
            "7615027634447060244", "7614784512190336276", "7614766447314619669",
            "7614746641676930324", "7613996173782764821", "7613909172769475861",
            "7613769097347632404", "7613642627367357716", "7613585104564194581",
            "7613277514047311125", "7613059025944464661"
        ]
    },
    {
        "record_id": "recvdmfocj6GuL",
        "tk_handle": "pae__2540",
        "video_ids": [
            "7276479378110958853", "7353195375810874641", "7558844639361486087",
            "7610783712556420360", "7607702510635093256", "7607699078834670866",
            "7607300541441445138", "7602911174849465607", "7602515706420235527",
            "7601021533254913298", "7600313561478368520"
        ]
    },
    {
        "record_id": "recvdmfoDPSdjA",
        "tk_handle": "prayutsunet2",
        "video_ids": [
            "7592437889280298247", "7448570294282816775", "7419348066421001490",
            "7615244222479748360", "7615229068765662482", "7614742244364422408",
            "7614051820205264136", "7613747147749543176", "7613564508795112711",
            "7613264808367951122", "7613051372711283986", "7612848178982898951"
        ]
    },
    {
        "record_id": "recvdmfpbV9ker",
        "tk_handle": "sawanitnim",
        "video_ids": [
            "7588475636373605652", "7588870777433328917", "7586608870110268693",
            "7615198750138436872", "7615121571732114706", "7615086772233637138",
            "7614854202199657736", "7614765066147187986", "7614762573468028178",
            "7614467992306371858", "7614347694017891591"
        ]
    },
    {
        "record_id": "recvdmfpFdQz7b",
        "tk_handle": "bum_thanatchaporn",
        "video_ids": [
            "7535598324603276562", "7574377261743312136", "7536154099314904338",
            "7615238721117375764", "7615230480690023701", "7615218596855221525",
            "7615203275482205460", "7615201004614585621", "7615194032045559060",
            "7615163945355119892", "7615135961671863572", "7615130408593804565"
        ]
    }
]

OUTPUT_DIR = Path.home() / ".openclaw/workspace/output/grids"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def process_creator(creator_data):
    """
    处理单个达人：获取最多 24 个视频封面，生成两张宫图并分别上传飞书

    - 宫图 1：第 1-12 张封面
    - 宫图 2：第 13-24 张封面（如果封面不足 12 张则跳过）
    """
    tk_handle = creator_data['tk_handle']
    record_id = creator_data['record_id']
    video_ids = creator_data['video_ids']
    
    print(f"\n{'='*70}")
    print(f"处理达人: {tk_handle}")
    print(f"Record ID: {record_id}")
    print(f"视频数量: {len(video_ids)}（最多取 {MAX_VIDEOS_PER_CREATOR} 个）")
    print(f"{'='*70}")
    
    try:
        # 步骤 1: 获取封面（最多 24 个）
        fetch_count = min(len(video_ids), MAX_VIDEOS_PER_CREATOR)
        print(f"⏳ 步骤 1/3: 获取封面（共 {fetch_count} 个）...")
        fetcher = OEmbedFetcher(cache_enabled=True, timeout=10)
        cover_urls = []
        
        for i, vid in enumerate(video_ids[:fetch_count], 1):
            print(f"  [{i}/{fetch_count}] 获取视频 {vid}...", end=" ")
            url = fetcher.fetch_cover_url(tk_handle, vid)
            if url:
                cover_urls.append(url)
                print("✅")
            else:
                print("❌")
        
        print(f"  封面获取结果: {len(cover_urls)}/{fetch_count}")
        
        if len(cover_urls) < MIN_COVER_COUNT:
            print(f"❌ 封面不足（需要至少 {MIN_COVER_COUNT} 个，实际 {len(cover_urls)} 个）")
            return {'status': 'failed', 'reason': 'insufficient_covers', 'tk_handle': tk_handle}
        
        # 步骤 2: 生成宫图（最多两张）
        print("⏳ 步骤 2/3: 生成宫图...")
        generator = GridCanvasGenerator()
        
        batch1 = cover_urls[:12]
        batch2 = cover_urls[12:24] if len(cover_urls) >= MIN_COVER_COUNT + 12 else []
        
        grid_paths = []
        
        for batch_idx, batch in enumerate([batch1, batch2], 1):
            if not batch:
                print(f"  ⏭️ 宫图 {batch_idx}：封面不足，跳过")
                continue
            
            print(f"  下载图片（宫图 {batch_idx}，共 {len(batch)} 张）...")
            images = generator.downloader.download_images_batch(batch)
            print(f"  下载结果: {len(images)}/{len(batch)}")
            
            if len(images) < MIN_COVER_COUNT:
                print(f"  ⚠️ 宫图 {batch_idx}：下载成功图片不足 {MIN_COVER_COUNT} 张，跳过")
                continue
            
            print(f"  创建宫格 {batch_idx}...")
            canvas = generator.create_canvas(images, max_images=12)
            
            output_path = OUTPUT_DIR / f"{tk_handle}_grid_{batch_idx}.jpg"
            canvas.save(output_path, format='JPEG', quality=85)
            print(f"  ✅ 宫图 {batch_idx} 已生成: {output_path}")
            grid_paths.append(str(output_path))
        
        if not grid_paths:
            print("❌ 所有宫图生成失败")
            return {'status': 'failed', 'reason': 'all_grids_failed', 'tk_handle': tk_handle}
        
        # 步骤 3: 上传到飞书（逐张上传）
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
        return {'status': 'failed', 'reason': str(e), 'tk_handle': tk_handle}


def main():
    """主函数"""
    print("="*70)
    print("批量处理达人 (第16-20个) - 生成宫图并上传飞书（每人两张）")
    print("="*70)
    print(f"待处理达人数量: {len(CREATORS)}\n")
    
    results = []
    success_count = 0
    failed_count = 0
    
    for i, creator in enumerate(CREATORS, 1):
        print(f"\n[{i}/{len(CREATORS)}]")
        result = process_creator(creator)
        results.append(result)
        
        if result['status'] == 'success':
            success_count += 1
        else:
            failed_count += 1
    
    # 保存结果
    summary = {
        'total': len(CREATORS),
        'success': success_count,
        'failed': failed_count,
        'results': results
    }
    
    summary_path = OUTPUT_DIR / 'batch_16_20_summary.json'
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*70}")
    print("处理完成!")
    print(f"{'='*70}")
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print(f"总计: {len(CREATORS)}")
    print(f"\n汇总结果已保存到: {summary_path}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
