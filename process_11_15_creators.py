#!/usr/bin/env python3
"""
处理第11-15个达人 - 生成两张宫图（各12张封面）并上传飞书
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

# 第11-15个达人的数据
CREATORS = [
    {
        "record_id": "recvdmeUrPskrR",
        "tk_handle": "peepo_89",
        "video_ids": [
            "7596980177905732872", "7599669256116079890", "7580209965349047570",
            "7615241573349051666", "7615241302401125640", "7615241086998449416",
            "7615238288558886162", "7615238170480823560", "7615238019859172615",
            "7615204183087582482", "7615203997611314440", "7615203814890654984"
        ]
    },
    {
        "record_id": "recvdmflCKk6Yi",
        "tk_handle": "nunoy2568",
        "video_ids": [
            "7609532959816781063", "7590015170546584840", "7585865787898989842",
            "7615242314520284424", "7615222387914706194", "7615211756260019463",
            "7615192998254529799", "7615178187374447880", "7615163625140915474",
            "7615148751203732743", "7615115118732463368", "7615112854319959303"
        ]
    },
    {
        "record_id": "recvdmfm3qMTpW",
        "tk_handle": "jai1jai",
        "video_ids": [
            "7528212716822023431", "7546836594611735815", "7563541378743930120",
            "7615234469313858824", "7615082118263213332", "7615081956157590805",
            "7615081683557223700", "7615081843435719957", "7615081620755811605",
            "7615081414207327508", "7615081243616546069", "7615076784224341269"
        ]
    },
    {
        "record_id": "recvdmfmE1pP4l",
        "tk_handle": "babell_22",
        "video_ids": [
            "7611571641394515220", "7595997050420989204", "7614978500465839380",
            "7615223167522491669", "7615219584768953621", "7614977684040551700",
            "7614977621000015124", "7614977383938084116", "7614868293471472916",
            "7614977358637944084", "7614855086220512532", "7614826978339835156"
        ]
    },
    {
        "record_id": "recvdmfn92C27b",
        "tk_handle": "aaalarnz.en",
        "video_ids": [
            "7610996072478969108", "7610210328797842708", "7561594810998754567",
            "7615237939970247956", "7615227130078661908", "7615212367592295701",
            "7615202083872165141", "7615189293853691156", "7615182092070309140",
            "7614845767794183444", "7614804773946101012", "7614748179212471573"
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
    print("批量处理达人 (第11-15个) - 生成宫图并上传飞书")
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
    
    summary_path = OUTPUT_DIR / 'batch_11_15_summary.json'
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
