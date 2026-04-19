#!/usr/bin/env python3
"""
处理第6-10个达人 - 生成两张宫图（各12张封面）并上传飞书
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

# 第6-10个达人的数据
CREATORS = [
    {
        "record_id": "recvdmeRSE5USn",
        "tk_handle": "ei.phyu.zin104",
        "video_ids": [
            "7605090247159975176", "7540487633571433735", "7540602420607339784",
            "7613237402957548807", "7612960224894274823", "7608499381146242322",
            "7606700642173799698", "7605971251965807890", "7605100005577329928",
            "7603618832590048520"
        ]
    },
    {
        "record_id": "recvdmeSq6EVWS",
        "tk_handle": "me_diary1992",
        "video_ids": [
            "7602348948669336839", "7602347983056817415", "7597873043360337160",
            "7614550397964897543", "7614550229274217746", "7614550014844472583",
            "7614549653505314066", "7614549402362842386", "7614548302742277394",
            "7614547942321425672", "7614547532881906962", "7614545453463407879"
        ]
    },
    {
        "record_id": "recvdmeSUHwbg6",
        "tk_handle": "zagasakai",
        "video_ids": [
            "7605209433072798984", "7513067323679345928", "7416975345271786759",
            "7615138241565461778", "7614956841210039560", "7614041862235786504",
            "7613737024410995975", "7613373006898924818", "7612964805779016968",
            "7612715394171669767", "7612632925330279688", "7612350955589324040"
        ]
    },
    {
        "record_id": "recvdmeTok7BBk",
        "tk_handle": "prasit2938",
        "video_ids": [
            "7584753301145636117", "7581053076639845652", "7552760716135107847",
            "7615203576612310293", "7615201902346751252", "7615200252764736789",
            "7615171942789877013", "7615170561508986132", "7615167068647787797",
            "7615110308163505429", "7615109052737064213", "7615107662715374868"
        ]
    },
    {
        "record_id": "recvdmeTZ0T2Nc",
        "tk_handle": "nong.nitcha",
        "video_ids": [
            "7602960527110851860", "7574009095640894741", "7608012528945270036",
            "7615232147355602196", "7615196284261043477", "7615120720674262293",
            "7615075623379193109", "7615047625376550165", "7614880033810697492",
            "7614864913873292565", "7614830447305968916", "7614806231038905621"
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
    print("批量处理达人 (第6-10个) - 生成宫图并上传飞书")
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
    
    summary_path = OUTPUT_DIR / 'batch_6_10_summary.json'
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
