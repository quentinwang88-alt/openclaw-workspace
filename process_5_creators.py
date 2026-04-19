#!/usr/bin/env python3
"""
⚠️ DEPRECATED - 此脚本已废弃

请使用新的批处理系统:
    python batch_runner.py batch_5

详见: BATCH_REFACTOR_README.md

---

原功能: 处理5个达人 - 生成两张宫图（各12张封面）并上传飞书
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

# 5个达人的数据（从浏览器提取）
CREATORS = [
    {
        "record_id": "recvdmcVxL2Q1m",
        "tk_handle": "soe..moe..kyi",
        "video_ids": [
            "7615214987761454354", "7615174225762012424", "7615171800246275346",
            "7615166157330337032", "7615162682148179208", "7615157483857775880",
            "7615150168308059399", "7615148832443157768", "7615145934657178888",
            "7615142976729337095", "7615132946638376210", "7615129098171485448"
        ]
    },
    {
        "record_id": "recvdmcW2lzIvC",
        "tk_handle": "fluke_0171",
        "video_ids": [
            "7591095075665415444", "7585539516694334728", "7577184916513852680",
            "7615220675459943700", "7615208925880732949", "7615198242724154644",
            "7615049244067499284", "7614867455936040213", "7614854989197905172",
            "7614847096738434325", "7614833990385618197", "7614825786238930196"
        ]
    },
    {
        "record_id": "recvdmeQnHq3xE",
        "tk_handle": "beholiday.official",
        "video_ids": [
            "7515757964192386324", "7372934152221363463", "7322827013578968338",
            "7615091896389537044", "7614389136476196114", "7614065509696392469",
            "7613767150834224404", "7612892153286577428", "7611773431226387733"
        ]
    },
    {
        "record_id": "recvdmeQQOzExE",
        "tk_handle": "narikapuy",
        "video_ids": [
            "7614767530426551570", "7614604520827440405", "7614585308444724501",
            "7614581246898834708", "7614517672776174869", "7614482932983516437",
            "7614467904607751444", "7614241842242014484", "7614161741047434517",
            "7614140316190362900", "7614104816805088533", "7614086924910873876"
        ]
    },
    {
        "record_id": "recvdmeRpcoCJw",
        "tk_handle": "maymapayyaa",
        "video_ids": [
            "7603760120081763600", "7615038912842534161", "7614565569550994706",
            "7614557713426664712", "7614543811607301392", "7614155621318839559",
            "7614133541428808976", "7614131133034401040", "7613773932109368592",
            "7613763477156416784", "7613679256819780865", "7613382137852579088"
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
        
        # 将封面分为两组（每组最多 12 张）
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
    print("批量处理达人 - 生成宫图并上传飞书")
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
    
    summary_path = OUTPUT_DIR / 'batch_5_summary.json'
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
    import sys
    print("\n" + "="*70)
    print("⚠️  警告: 此脚本已废弃")
    print("="*70)
    print("请使用新的批处理系统:")
    print("  python batch_runner.py batch_5")
    print("\n详见: BATCH_REFACTOR_README.md")
    print("="*70)
    
    response = input("\n是否继续使用旧脚本? (y/N): ")
    if response.lower() != 'y':
        print("已取消")
        sys.exit(0)
    
    print("\n继续使用旧脚本...\n")
    main()
